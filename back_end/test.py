""" Author: Dishant Mittal
	Component of IOT-Hack
	Created on 23/1/18
	For testing the modules
	"""

import unittest
import cosine_similarity
import split_into_days


class TestProject(unittest.TestCase):
    def test_dot_product(self):
        self.assertEqual(cosine_similarity.dot_product([1, 2, 3], [3, 2, 1]), 10)
        self.assertEqual(cosine_similarity.dot_product([2, 2, 3], [3, 2, 1]), 13)
        self.assertEqual(cosine_similarity.dot_product([1, 5, 3], [1, 2, 2]), 17)
        self.assertEqual(cosine_similarity.dot_product([2, 3], [3, 2]), 12)
        self.assertEqual(cosine_similarity.dot_product([5, 2, 8], [7, 2, 3]), 63)

    def test_cosine_measure(self):
        self.assertEqual(round(cosine_similarity.cosine_measure([1, 2, 3], [3, 2, 1]),3), 0.714)
        self.assertEqual(round(cosine_similarity.cosine_measure([1, 2, 3], [3, 2332, 1]),3), 0.535)
        self.assertEqual(round(cosine_similarity.cosine_measure([121, 2, 3], [333, 2, 1]),3), 1.0)
        self.assertEqual(round(cosine_similarity.cosine_measure([1, 12213, 3], [834, 2, 1]),3), 0.002)
        self.assertEqual(round(cosine_similarity.cosine_measure([321, 2003, 23], [3, 2, 123]),3), 0.031)

    def test_convert_timestamp(self):
        self.assertEqual(split_into_days.convert_timestamp(1473732300000L), '09-12-16-Monday')
        self.assertEqual(split_into_days.convert_timestamp(1423722300000L), '02-12-15-Thursday')
        self.assertEqual(split_into_days.convert_timestamp(1443232300000L), '09-25-15-Friday')
        self.assertEqual(split_into_days.convert_timestamp(1453872300000L), '01-27-16-Wednesday')
        self.assertEqual(split_into_days.convert_timestamp(1493342300000L), '04-27-17-Thursday')


if __name__ == '__main__':
    unittest.main()
