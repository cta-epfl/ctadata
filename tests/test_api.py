import contextlib
import io
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from ctadata.api import APIClient, StorageException


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


class TransferTokenRedactionTests(unittest.TestCase):
    def test_failed_transfer_does_not_log_or_raise_token(self):
        token = 'sensitive-test-token'
        client = APIClient.__new__(APIClient)
        client._token = token
        client.dcache_url = 'https://example.invalid'
        failure = SimpleNamespace(
            returncode=1,
            stdout=f'output contains {token}',
            stderr=f'error contains {token}',
        )

        for transfer in ('fetch_and_save_file', 'upload_file'):
            with self.subTest(transfer=transfer):
                with patch('ctadata.api.subprocess.run', return_value=failure):
                    with self.assertLogs('ctadata.api', level='ERROR') as logs:
                        with self.assertRaises(StorageException) as error:
                            if transfer == 'fetch_and_save_file':
                                client.fetch_and_save_file('missing-file')
                            else:
                                client.upload_file(
                                    'local-file', 'missing-file')

                diagnostics = '\n'.join(logs.output) + str(error.exception)
                self.assertNotIn(token, diagnostics)
                self.assertIn('******', diagnostics)


if __name__ == '__main__':
    unittest.main()
