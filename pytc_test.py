import pytc
db = pytc.HDB('casket.hdb', pytc.HDBOWRITER | pytc.HDBOCREAT)
db.put('potato', 'potatis')
db.put('carrot', 'morot')
db.put('banana', 'banan')
assert db.get('carrot') == 'morot'
db.put("digest", "1")
print db.get("digest")
db.addint("digest", 2)
print db.get("digest")
#assert db.get('digest') == 1
