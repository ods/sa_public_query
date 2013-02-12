#!/usr/bin/python

from sqlalchemy.orm.query import Query
from sqlalchemy.sql import ClauseElement
from sqlalchemy import cast, Boolean
from sqlalchemy.orm.util import _class_to_mapper


class PublicQuery(Query):

    '''
    Filters all queries by publicity condition for each participating mapped
    class. Attribute "public" of mapped class (if present) should be either
    boolean column or @hybrid_property providing publicity criterion clause for
    the class and boolean (convertable to boolean) value for instance of the
    class.

    A version from recipe combined with our own vision
    http://www.sqlalchemy.org/trac/wiki/UsageRecipes/PreFilteredQuery
    '''

    def get(self, ident):
        obj = Query.get(self, ident)
        if obj is not None and getattr(obj, 'public', True):
            return obj
        # Other option:
        # override get() so that the flag is always checked in the 
        # DB as opposed to pulling from the identity map. - this is optional.
        #return Query.get(self.populate_existing(), ident)

    def __iter__(self):
        return Query.__iter__(self.private())

    def from_self(self, *ent):
        # override from_self() to automatically apply
        # the criterion too.   this works with count() and
        # others.
        return Query.from_self(self.private(), *ent)

    def private(self):
        query = self
        for query_entity in self._entities:
            for entity in query_entity.entities:
                if hasattr(entity, 'parententity'):
                    entity = entity.parententity
                try:
                    cls = _class_to_mapper(entity).class_
                except AttributeError:
                    # XXX For tables, table columns
                    #pass
                    raise
                else:
                    crit = getattr(cls, 'public', None)
                    if crit is not None:
                        if not isinstance(crit, ClauseElement):
                            # This simplest safe way to make bare boolean column
                            # accepted as expression.
                            crit = cast(crit, Boolean)
                        # XXX It's dangerous since it can mask errors
                        query = query.enable_assertions(False).filter(crit)
        return query


if __name__=='__main__':
    from base import run_test
    run_test(PublicQuery)
