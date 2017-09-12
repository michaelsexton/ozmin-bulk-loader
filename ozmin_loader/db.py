import cx_Oracle
import math

class OzminConnection:

  def __init__(self, user = "", password = "", server = ""):
    self.connection = cx_Oracle.connect('test','test','test')
    self.cursor = self.connection.cursor()

  def get_or_create_deposit(self, eno, entityid):
    if not math.isnan(eno):
      sql = "select eno from a.entities where eno = {0}".format(int(eno))
    else:
      sql = "select eno from a.entities where entityid = '{0}' and entity_type = 'MINERAL DEPOSIT'".format(entityid)
    self.cursor.execute(sql)
    results = self.cursor.fetchall()
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

    self.cursor.execute(sql)
    results = self.cursor.fetchall()
    if len(results) == 1 :
      return results[0][0]
    elif len(results) > 1:
      print("ERROR {0} - {1}".format(eno, entityid))
    elif len(results) == 0:
      print("No results: {0}, deposit: {1}".format(entityid, deposit_eno))
   
  def add_resources(self, zone_eno, record_date, pvr, pbr, mrs, idr, ifr, comment):
    record_date = record_date.strftime('%d-%b-%Y')
    query = "select resourceno from mgd.resources where eno = {0} and recorddate = '{1}'".format(zone_eno, record_date)
    self.cursor.execute(query)
    results = self.cursor.fetchall()
    if len(results) > 0:
      print("Already resources data here!")
      return None
      
    fields = "RESOURCENO, "
    values = "(select max(resourceno) from mgd.resources) + 1, "
    categories = {"PVR":pvr, "PBR":pbr, "MRS":mrs, "IDR":idr, "IFR":ifr }
    for category in categories:
      if not math.isnan(categories[category]):
        fields = fields + "{0}, ".format(category)
        values = values + "{0}, ".format(categories[category])
    fields = fields + "ENO, UNIT_QUANTITY, INCLUSIVE, REC_RECOVERABLE, RECORDDATE, REMARK, ANO, ACCESS_CODE, CURRENT_R, ACTIVITY_CODE"
    values = values + "{0}, 'Mt', 'Y', 'Y', '{1}', '{2}', 168, 'O', 'Y', 'A'".format(zone_eno, record_date, comment)
    
    sql = "insert into mgd.resources ({0}) values ({1})".format(fields, values)
    self.cursor.execute(sql)
    
    self.cursor.execute(query)
    results = self.cursor.fetchall()
    if len(results) == 1:
      self.connection.commit()
      return results[0][0]
    else:
      print("Something went wrong!")
      self.connection.rollback()
      return None
    
  def add_grades(self, resourceno, commodid, pvr, pbr, mrs, idr, ifr):
    query = "select rescommno from mgd.resource_grades where commodid = '{0}' and resourceno = {1}".format(commodid,resourceno)
    self.cursor.execute(query)
    results = self.cursor.fetchall()
    if len(results) > 0:
      print("Already resource grades data here!")
      return None
    fields = "RESCOMMNO, "
    values = "(select max(RESCOMMNO) from mgd.resource_grades) + 1, "
    categories = {"PVR":pvr, "PBR":pbr, "MRS":mrs, "IDR":idr, "IFR":ifr }
    classes = {"PVR":"MDE", "PBR":"IDE", "MRS":"MDE", "IDR":"IDE", "IFR":"IFU" }
    for category in categories:
      if not math.isnan(categories[category]):
        ga_class = category + "_CLASS1"
        ga_pcnt = category + "_PCNT1"
        fields = fields + "{0}, {1}, {2}, ".format(category, ga_class, ga_pcnt)
        values = values + "{0}, '{1}', 100, ".format(categories[category], classes[category])
    fields = fields + "COMMODID, UNIT_GRADE, RESOURCENO, ENTRYDATE, ENTEREDBY,ANO"
    values = values + "'{0}', '%', {1}, SYSDATE, 'MSEXTON1', 168".format(commodid, resourceno)
    sql = "insert into mgd.resource_grades ({0}) values ({1})".format(fields, values)
    self.cursor.execute(sql)
    
    self.cursor.execute(query)
    results = self.cursor.fetchall()
    if len(results) == 1:
      self.connection.commit()
      return results[0][0]
    else:
      print("Something went wrong!")
      self.connection.rollback()
      return None
  
    
    
    
 