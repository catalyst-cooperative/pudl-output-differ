import unittest
 
from pudl_output_differ import types


class MyType(types.TypeDef):
    """Test type."""
    a: str
    b: str
    def __str__(self):
        return f"MyType({self.a}:{self.b})"
    

class Zeta(types.TypeDef):
    value: int
    def __str__(self):
        return f"Zeta({self.value})"
    

class TestObjectPath(unittest.TestCase):

    def test_extend_and_sort(self):
        p1 = types.ObjectPath.from_nodes(
            MyType(a="foo", b="bar"),
            Zeta(value=10),
        )
        self.assertEqual("MyType(foo:bar)/Zeta(10)", str(p1))

        # new instances with same values should be equal
        self.assertEqual(p1.extend(Zeta(value=3)), p1.extend(Zeta(value=3)))

        p20 = p1.extend(Zeta(value=20))
        p15 = p1.extend(Zeta(value=15))
        self.assertEqual("MyType(foo:bar)/Zeta(10)/Zeta(15)", str(p15))
        self.assertEqual("MyType(foo:bar)/Zeta(10)/Zeta(20)", str(p20))
        self.assertLess(p15, p20)
        self.assertLess(p1, p15)
        self.assertLess(p1, p20)

        self.assertLess(p15.extend(Zeta(value=10)), p20.extend(Zeta(value=5)))
        self.assertLess(p15.extend(Zeta(value=10)), p20.extend(Zeta(value=20)))

        # p15 < p20, so anything appended to p15 should be less than anything appended to p20.
