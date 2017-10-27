'''
Created on Apr 7, 2010
 
@author: marat
'''
 
from scalarizr.util import cryptotool, init_tests
import unittest
import binascii
 
class Test(unittest.TestCase):
 
    def test_keygen(self):
        print cryptotool.keygen(40)
 
    def test_crypto(self):
        key = binascii.a2b_base64("yyUKYpE1IsMCBdmSAs7FpxWwRszfSbPBqD+Obi3P7Cg28SpLuqB4eA==")
        #s = "vtpGn1C4TbvR/Z7P4qVRij/UGYwcB16D5gTR18cOF+AZhgJqSM/LkVXWAMxFWs5NK83EyZ42/6Y0kkpMJgMGw9IrRBawtsrwI8+5Ty1r+5Wi8KsMXY5hbUmujbwghuVOGpWR4WrTQ/kUARo5oE8U196t97jXVp6r7zSgPe94MyGSHw0+EDZzYXS/q1jePoHaZSSD+meyts9KGzMHAAvvN2E1"
 
        se = cryptotool.encrypt("12345678", key)
        print se
 
        so = cryptotool.decrypt(se, key)
        self.assertEqual(so, "12345678")
 
        print so
 
 
if __name__ == "__main__":
    init_tests()
    unittest.main()
 
