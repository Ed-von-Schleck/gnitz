import unittest
from gnitz.storage.errors import GnitzError, StorageError, BoundsError

class TestErrors(unittest.TestCase):
    def test_exception_hierarchy(self):
        self.assertTrue(issubclass(StorageError, GnitzError))
        self.assertTrue(issubclass(BoundsError, StorageError))

    def test_raise_catch(self):
        try:
            # RPython compat: Must provide explicit arguments now
            raise BoundsError(0, 0, 0)
        except StorageError:
            pass
        except:
            self.fail("BoundsError not caught as StorageError")

if __name__ == '__main__':
    unittest.main()
