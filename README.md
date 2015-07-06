# sqliteKV
store key/value in sqlite 

from sqliteKV import DB

tab = DB(':memory:').table()
tab.put('a', 1)
tab.put('b', 2)
tab.put('c', 2)
tab._update('c', [4,5,'A'])
print tab.items()
