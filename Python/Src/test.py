import hashlib


merged_vc = {'A':3, 'B':4, 'C':2}
masterid = 'D'
merged_vc[masterid] = merged_vc.get(masterid, 0) + 1
print merged_vc

testip = '192.168.56.1'
print hashlib.sha256(testip).hexdigest()		