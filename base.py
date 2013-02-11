import unittest
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base


UserAddressesBase = declarative_base()


class User(UserAddressesBase):
    __tablename__ = 'user'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    public = Column(Boolean, nullable=False)
    addresses = relation("Address", backref="user")


class Address(UserAddressesBase):
    __tablename__ = 'address'

    id = Column(Integer, primary_key=True)
    email = Column(String)
    user_id = Column(Integer, ForeignKey('user.id'))
    public = Column(Boolean, nullable=False)


class UserAddressesTest(unittest.TestCase):
    '''
    Simple set of tests with the same set of initial objects from original
    recipe at http://www.sqlalchemy.org/trac/wiki/UsageRecipes/PreFilteredQuery
    '''

    QUERY_CLS = None  # Must be set before running tests

    def setUp(self):
        engine = create_engine('sqlite://')#, echo=True)
        UserAddressesBase.metadata.create_all(engine)
        # Some solutions doen't allow creating objects with PublicQuery, so we
        # setup separate session for it.
        # dba = (all) session with standard Query class
        # dbp = (public) session with tested PublicQuery class
        self.dba = sessionmaker(bind=engine)()
        self.dba.add_all([
            User(name='u1', public=True,
                 addresses=[Address(email='u1a1', public=True),
                            Address(email='u1a2', public=True)]),
            User(name='u2', public=True,
                 addresses=[Address(email='u2a1', public=False),
                            Address(email='u2a2', public=True)]),
            User(name='u3', public=False,
                 addresses=[Address(email='u3a1', public=False),
                            Address(email='u3a2', public=False)]),
            User(name='u4', public=False,
                 addresses=[Address(email='u4a1', public=False),
                            Address(email='u4a2', public=True)]),
            User(name='u5', public=True,
                 addresses=[Address(email='u5a1', public=True),
                            Address(email='u5a2', public=False)])
        ])
        self.dba.commit()
        self.dbp = sessionmaker(bind=engine, query_cls=self.QUERY_CLS)()

    def tearDown(self):
        self.dba.close()
        self.dbp.close()

    def test_public(self):
        # This test doesn't depend on initial state of DB
        for user in self.dbp.query(User):
            self.assertTrue(user.public)
        for addr in self.dbp.query(Address):
            self.assertTrue(addr.public)

#assert entries == [
#    (u'u1a1', u'u1'),
#    (u'u1a2', u'u1'),
#    (u'u2a2', u'u2'),
#    (u'u4a2', None),
#    (u'u5a1', u'u5'),
#]
#
#a1 = sess.query(Address).filter_by(email='u1a1').one()
#a1_user_id = a1.user.id
#assert sess.query(User).get(a1_user_id) is not None
#a1.user.public = False
#sess.commit()
#
#assert a1.user is None
#assert sess.query(User).get(a1_user_id) is None
#
#assert sess.query(User).order_by(User.name).first().name=='u2'
#
#assert list(sess.query(User).values(User.name)) == [('u2',), ('u5',)]
#assert sess.query(User.name).all() == [('u2',), ('u5',)]
#assert sess.query(User).count()==2
#
#
## XXX The following assertions fail:
#
#assert sess.query(User.name).join(User.addresses).filter(Address.email=='u2a1').all()==[]
#assert sess.query(User.name).filter(User.addresses.any(email='u2a1')).all()==[]
#assert sess.query(User.name, Address.email).join(Address.user).all()==[('u2', 'u2a2'), ('u5', 'u5a1')]
#assert sess.query(Address.email, User.name).join(Address.user).all()==[('u2a2', 'u2'), ('u5a1', 'u5')]


def run_test(query_cls):
    UserAddressesTest.QUERY_CLS = query_cls
    suite = unittest.TestLoader().loadTestsFromTestCase(UserAddressesTest)
    unittest.TextTestRunner().run(suite)
