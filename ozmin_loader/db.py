import cx_Oracle
import math

class OzminConnection:

  def __init__(self, user = "", password = "", server = ""):
   

  def get_or_create_deposit(self, eno, entityid):
    if not math.isnan(eno):
      sql = "select eno from a.entities where eno = {0}".format(int(eno))
    else:
      sql = "select eno from a.entities where entityid = '{0}' and entity_type = 'MINERAL DEPOSIT'".format(entityid)
    cursor = self.connection.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    if len(results) == 1 :
      return results[0][0]
    elif len(results) > 1:
      print("ERROR {0} - {1}".format(eno, entityid))
    elif len(results) == 0:
      print("Returned: {0} - {1}".format(results, entityid))
      
  def get_or_create_zone(self, deposit_eno, zone_eno, entityid):
    if not math.isnan(zone_eno):
      sql = "select eno from a.entities where eno = {0} and parent = {1}".format(int(zone_eno),deposit_eno)
    else:
      sql = "select eno from a.entities where entityid = '{0}' and parent={1} and entity_type = 'MINERALISED ZONE'".format(entityid, deposit_eno)
    cursor = self.connection.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    if len(results) == 1 :
      return results[0][0]
    elif len(results) > 1:
      print("ERROR {0} - {1}".format(eno, entityid))
    elif len(results) == 0:
      print("No results: {0}, deposit: {1}".format(entityid, deposit_eno))
   
  def add_resources(self, zone_eno, record_date, pvr, pbr, mrs, idr, ifr, comment):
    fields = "RESOURCENO, "
    values = "(select max(resourceno) from mgd.resources) + 1, "
    categories = {"PVR":pvr, "PBR":pbr, "MRS":mrs, "IDR":idr, "IFR":ifr }
    for category in categories:
      if not math.isnan(categories[category]):
        fields = fields + "{0}, ".format(category)
        values = values + "{0}, ".format(categories[category])
    fields = fields + "ENO, RECORDDATE, REMARK, ANO, ACCESS_CODE, CURRENT_R, ACTIVITY_CODE"
    values = values + "{0}, '{1}', '{2}', 168, 'O', 'Y', 'A'".format(zone_eno, record_date.strftime('%d-%b-%Y'), comment)
    sql = "insert into mgd.resources ({0}) values ({1});".format(fields, values)
    print(sql)