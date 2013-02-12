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
                            Address(email='u5a2', public=False)]),
            User(name='u6', public=True,
                 addresses=[Address(email='u6a1', public=False),
                            Address(email='u6a2', public=False)]),
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

    def test_query_iter(self):
        names = [u.name for u in self.dbp.query(User)]
        self.assertEqual(names, ['u1', 'u2', 'u5', 'u6'])
        emails = [a.email for a in self.dbp.query(Address)]
        self.assertEqual(emails, ['u1a1', 'u1a2', 'u2a2', 'u4a2', 'u5a1'])

    def test_query_field(self):
        names = set(n for (n,) in self.dbp.query(User.name))
        self.assertEqual(names, set(['u1', 'u2', 'u5', 'u6']))
        emails = set(e for (e,) in self.dbp.query(Address.email))
        self.assertEqual(emails, set(['u1a1', 'u1a2', 'u2a2', 'u4a2', 'u5a1']))

    def test_relation_list(self):
        for name, emails in {'u1': ['u1a1', 'u1a2'],
                             'u2': ['u2a2'],
                             'u5': ['u5a1'],
                             'u6': []}.items():
            user = self.dbp.query(User).filter_by(name=name).scalar()
            self.assertEqual(set(a.email for a in user.addresses), set(emails))

    def test_relation_scalar(self):
        for email, name in {'u1a1': 'u1',
                            'u1a2': 'u1',
                            'u2a2': 'u2',
                            'u4a2': None,
                            'u5a1': 'u5'}.items():
            addr = self.dbp.query(Address).filter_by(email=email).scalar()
            if name is None:
                self.assertIsNone(addr.user)
            else:
                self.assertEqual(addr.user.name, name)

    def test_count(self):
        self.assertEqual(self.dbp.query(User).count(), 4)
        self.assertEqual(self.dbp.query(Address).count(), 5)

    def test_func_count(self):
        self.assertEqual(self.dbp.query(func.count(User.id)).scalar(), 4)
        self.assertEqual(self.dbp.query(func.count(Address.id)).scalar(), 5)

    def test_get(self):
        for user_id, in self.dba.query(User.id)\
                    .filter(User.name.in_(['u1', 'u2', 'u5', 'u6'])):
            user = self.dbp.query(User).get(user_id)
            self.assertIsNotNone(user)
        for user_id, in self.dba.query(User.id)\
                    .filter(User.name.in_(['u3', 'u4'])):
            user = self.dbp.query(User).get(user_id)
            self.assertIsNone(user)

    def test_relation_after_change(self):
        user = self.dbp.query(User).filter_by(name='u1').scalar()
        self.assertEqual(len(user.addresses), 2)
        addr1, addr2 = user.addresses
        self.assertIsNotNone(addr1.user)
        self.assertIsNotNone(addr2.user)
        addr2.public = False
        self.dbp.commit()
        self.assertEqual(len(user.addresses), 1)
        self.assertIsNotNone(addr1.user)
        user.public = False
        self.dbp.commit()
        self.assertIsNone(addr1.user)

    def test_private_by_public_join(self):
        query = self.dbp.query(User).join(User.addresses)\
                    .filter(Address.email=='u4a2')
        self.assertEqual(query.count(), 0)
        self.assertEqual(query.all(), [])

    def test_private_by_public_exists(self):
        query = self.dbp.query(User).filter(User.addresses.any(email='u4a2'))
        self.assertEqual(query.count(), 0)
        self.assertEqual(query.all(), [])

    def test_public_by_private_join(self):
        query = self.dbp.query(User).join(User.addresses)\
                    .filter(Address.email=='u2a1')
        self.assertEqual(query.count(), 0)
        self.assertEqual(query.all(), [])

    def test_public_by_private_exists(self):
        query = self.dbp.query(User).filter(User.addresses.any(email='u2a1'))
        self.assertEqual(query.count(), 0)
        self.assertEqual(query.all(), [])

    def test_join_pairs(self):
        query = self.dbp.query(User.name, Address.email).join(Address.user)
        self.assertEqual(set(query.all()),
                         set([('u1', 'u1a1'),
                              ('u1', 'u1a2'),
                              ('u2', 'u2a2'),
                              ('u5', 'u5a1')]))

    def test_relation_group_count(self):
        query = self.dbp.query(User.name, func.count(Address.id))\
                        .outerjoin(User.addresses).group_by(User.id)
        count_by_name = dict(query.all())
        self.assertEqual(count_by_name, {'u1': 2, 'u2': 1, 'u5': 1, 'u6': 0})


def run_test(query_cls):
    UserAddressesTest.QUERY_CLS = query_cls
    suite = unittest.TestLoader().loadTestsFromTestCase(UserAddressesTest)
    unittest.TextTestRunner().run(suite)
