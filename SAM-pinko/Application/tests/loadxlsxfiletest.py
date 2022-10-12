import unittest

from app import lambda_handler

class TestReturnsData(unittest.TestCase):

    def test_returns_bad_request(self):
        self.assertEqual({"status": "BAD"},lambda_handler('3423423'))

    def test_loads_correct_file(self):
        pid = 4392500
        answer = lambda_handler(pid)
        self.assertTrue(answer['status'] == "OK")


    def test_handles_errors(self):
        
        self.assertTrue()

if __name__ == '__main__':
    unittest.main()