#!/usr/bin/python

from sqlalchemy.orm.query import Query
from sqlalchemy.sql import ClauseElement
from sqlalchemy import cast, Boolean
from sqlalchemy.orm.util import _class_to_mapper
from sqlalchemy import exc as sa_exc
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.sql import util as sql_util


class PublicQuery(Query):

    '''
    Filters all queries by publicity condition for each participating mapped
    class. Attribute "public" of mapped class (if present) should be either
    boolean column or @hybrid_property providing publicity criterion clause for
    the class and boolean (convertable to boolean) value for instance of the
    class.

    This solution is used by STM in several projects. It's usually based on
    patched version of SQLAlchemy.
    '''

    def __init__(self, entities, *args, **kwargs):
        Query.__init__(self, entities, *args, **kwargs)
        for entity in entities:
            if hasattr(entity, 'parententity'):
                entity = entity.parententity
            try:
                cls = _class_to_mapper(entity).class_
            except AttributeError:
                # XXX For tables, table columns
                pass
            else:
                crit = getattr(cls, 'public', None)
                if crit is not None:
                    if not isinstance(crit, ClauseElement):
                        # This simplest safe way to make bare boolean column
                        # accepted as expression.
                        crit = cast(crit, Boolean)
                    query = self.filter(crit)
                    self._criterion = query._criterion

    # XXX The rest method usually are applied as patch to SQLAlchemy code (see
    # separate file). Here we copied patched versions of method to avoid affect
    # on tests of other solutions.

    def _no_criterion_condition(self, meth):
        if not self._enable_assertions:
            return
        # XXX ods: this method is mostly used in _load_on_ident which is hacked
        # to use current criterion
        assert meth=='get' or self._criterion is None
        #if self._criterion is not None or \
        if \
                self._statement is not None or self._from_obj or \
                self._limit is not None or self._offset is not None or \
                self._group_by or self._order_by or self._distinct:
            raise sa_exc.InvalidRequestError(
                                "Query.%s() being called on a "
                                "Query with existing criterion. " % meth)

        self._from_obj = ()
        #self._statement = self._criterion = None
        self._statement = None
        self._order_by = self._group_by = self._distinct = False

    def _load_on_ident(self, key, refresh_state=None, lockmode=None,
                                        only_load_props=None):
        """Load the given identity key from the database."""

        lockmode = lockmode or self._lockmode

        if key is not None:
            ident = key[1]
        else:
            ident = None

        if refresh_state is None:
            q = self._clone()
            q._get_condition()
        else:
            q = self._clone()

        if ident is not None:
            mapper = self._mapper_zero()

            (_get_clause, _get_params) = mapper._get_clause

            # None present in ident - turn those comparisons
            # into "IS NULL"
            if None in ident:
                nones = set([
                            _get_params[col].key for col, value in
                             zip(mapper.primary_key, ident) if value is None
                            ])
                _get_clause = sql_util.adapt_criterion_to_null(
                                                _get_clause, nones)

            _get_clause = q._adapt_clause(_get_clause, True, False)
            # XXX ods: use current criterion
            if q._criterion is not None:
                q._criterion &= _get_clause
            else:
                q._criterion = _get_clause

            params = dict([
                (_get_params[primary_key].key, id_val)
                for id_val, primary_key in zip(ident, mapper.primary_key)
            ])

            q._params = params

        if lockmode is not None:
            q._lockmode = lockmode
        q._get_options(
            populate_existing=bool(refresh_state),
            version_check=(lockmode is not None),
            only_load_props=only_load_props,
            refresh_state=refresh_state)
        q._order_by = None

        try:
            return q.one()
        except orm_exc.NoResultFound:
            return None



if __name__=='__main__':
    from base import run_test
    run_test(PublicQuery)
