import cx_Oracle

def get_or_create_deposit(eno, entityid):
  if eno:
    sql = 'select eno from a.entities where eno = {0}'.format(eno)
  else
    sql = 'select eno from a.entities where entityid = {0}'.format(entityid)
  