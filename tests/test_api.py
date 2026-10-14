import contextlib
import io
import unittest

from ctadata.api import APIClient


class PrintTokenTests(unittest.TestCase):
    def test_does_not_print_cached_token_when_token_file_is_missing(self):
        client = APIClient.__new__(APIClient)
        client.cta_token_file = '/missing/token'
        client._token = 'cached-secret-token'
        stdout = io.StringIO()
        stderr = io.StringIO()

        with contextlib.redirect_stdout(stdout), \
                contextlib.redirect_stderr(stderr):
            client.print_token()

        self.assertEqual(stdout.getvalue(), '')
        self.assertNotIn('cached-secret-token', stderr.getvalue())
        self.assertIn('Token not found', stderr.getvalue())


if __name__ == '__main__':
    unittest.main()
