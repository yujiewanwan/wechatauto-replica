import unittest

from wechatauto.contact_profiles import parse_extra_buffer


def message(field, value):
    key = (field << 3) | 2
    prefix = bytes([key]) if key < 128 else bytes([(key & 127) | 128, key >> 7])
    return prefix + bytes([len(value)]) + value


class ContactProfileTests(unittest.TestCase):
    def test_multiple_phones_and_labels(self):
        phones = message(2, message(1, b'18969439816')) + message(2, message(1, b'021-12345678'))
        self.assertEqual(parse_extra_buffer(message(14, phones) + message(30, b'2,3')),
                         (['18969439816', '021-12345678'], [2, 3]))

    def test_clear_and_unknown_fields(self):
        self.assertEqual(parse_extra_buffer(message(30, b'') + message(40, b'future')), ([], []))

    def test_corruption_must_not_clear_data(self):
        for data in [b'\x72\x11\x00', message(30, b'not-an-id'), b'\x70\x01']:
            with self.assertRaises(ValueError):
                parse_extra_buffer(data)


if __name__ == '__main__':
    unittest.main()
