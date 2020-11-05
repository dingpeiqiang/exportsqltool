"""
---------------------------------------
 Author:dingpq
 Date： 2020-10-29 15:51
---------------------------------------
"""
import  cx_Oracle
import re
from PyQt5.QtWidgets import QMessageBox

dbstrlist = {} #数据库配置串
dbconnectlist = {}#数据库链接
from sys import modules
#连接Oracle数据库
class oracleOperation():
    #初始化连接数据库
    def openOracleConn(self):
        self.readdbpropfile()
        global dbconnectlist

        for key in dbstrlist.keys():
            db = dbstrlist[key]
            user = db[1].split("/")[0]
            passwd = db[1].split("/")[1]
            url = db[2]
            print(user,'-',passwd,'-',url)
            #建立Oracle远程连接
            highway=cx_Oracle.connect(user,passwd,url)
            dbconnectlist[key] = highway

    #读取数据库配置文件
    def readdbpropfile(self):
        global  dbstrlist
        index = 0
        with open("./db.properties", 'r') as f:
            for eachline in f:
                line = eachline.strip()
                # 空行和注释行 跳过
                if not len(line) or line.startswith('#'):
                    continue
                index += 1
                strlist = line.split("@")
                dbstrlist[strlist[0]] = strlist
                print(index, '--', eachline)


    #获取所有依赖的key
    def findall_keys(self,key_str):
        keys = set()
        #开始匹配key
        pattern = re.compile(r'[$][{](\w+)[}]')
        match = pattern.findall(key_str)
        if match:
            keys = keys|set(match)

        return keys    
        
        
    """
     ---------------------------
     用于获取data_key 在定义表中content字段和condiions字段 依赖的key列表
     ---------------------------
    """

    def get_depnd_keys(self,connection,data_key):

        key_list=set()
        cursor = connection.cursor();
        sql = 'select conditions,content_clob from pd_cfgdata_dict where data_key =:data_key'
        result = cursor.execute(sql,{'data_key':data_key})
        row = cursor.fetchone()  # 得到所有数据集
        if not row:
            return key_list
        print('=======',data_key,'的结果集：',row)
        if not row[1]:
            print("当前data_kye:"+data_key+" 的content_clob为空，请核查配置！")
            return key_list
        content_clob = row[1].read()
        conditions = row[0]
        print('=======conditions：', conditions)
        print('=======content_clob：', content_clob)
        cursor.close()

        #开始匹配key
        key_list = key_list | self.findall_keys(content_clob)   
        if conditions:    
            key_list = key_list|self.findall_keys(conditions)
            
        for k in key_list:
            key_list = key_list|self.get_depnd_keys(connection,k)    
        return key_list



    """
     ---------------------------
     用于导出模板或者接口关联的data_type =S 的脚本
     ---------------------------
    """

    def export_S_sql(self,tempOrInter,connection):

        key_list =[]
        cursor = connection.cursor();
        sql = """
        select exportclobsql(data_key) from dbpmsadm.pd_cfgdata_dict where data_key in (
        select key from  dbpmsadm.pd_cfginterfacestep_rel where interface_id in (
        select interface_id from  dbpmsadm.pd_cfgtemplateinterface_rel where template_id =:tempOrInter
        union
        select interface_id from  dbpmsadm.pd_cfginterface_dict where interface_id  =:tempOrInter

        )
        )"""
        result = cursor.execute(sql,{'tempOrInter':tempOrInter})
        rows = cursor.fetchall()  # 得到所有数据集
        if not rows:
            return key_list
        for s in rows:
            key_list.append(s[0].read())
        
        cursor.close()
        return key_list

    """
     ---------------------------
     用于导出模板配置除了pd_cfginterfacestep_rel配置的key外 其他依赖的key
     ---------------------------
    """    
    def export_D_sql(self,tempOrInter,connection):
        dependence_kyes = set()
        
        ## 获取default_value 依赖的key
        with connection.cursor() as cursor:
            default_value_sql = """
            select distinct default_value from dbpmsadm.pd_cfgelement_dict where element_key in (
            select key from  dbpmsadm.pd_cfginterfacestep_rel where interface_id in (
            select interface_id from  dbpmsadm.pd_cfgtemplateinterface_rel where template_id  =:tempOrInter
            union
            select interface_id from  dbpmsadm.pd_cfginterface_dict where interface_id  =:tempOrInter
            )
            ) and default_value is not null
            """
            result = cursor.execute(default_value_sql,{'tempOrInter':tempOrInter})
            rows = cursor.fetchall()  # 得到所有数据集
            for s in rows:
                dependence_kyes = dependence_kyes|self.findall_keys(s[0])
            
        ## 获取value_source依赖的key
        with connection.cursor() as cursor:        
            value_source_sql = """
            select distinct value_source from dbpmsadm.pd_cfgelement_dict where element_key in (
            select key from  dbpmsadm.pd_cfginterfacestep_rel where interface_id in (
            select interface_id from  dbpmsadm.pd_cfgtemplateinterface_rel where template_id =:tempOrInter
            union
            select interface_id from  dbpmsadm.pd_cfginterface_dict where interface_id  =:tempOrInter

            )
            )and value_source is not null
           """
            result = cursor.execute(value_source_sql,{'tempOrInter':tempOrInter})
            rows = cursor.fetchall()  # 得到所有数据集
            for s in rows:
                dependence_kyes = dependence_kyes|self.findall_keys(s[0])
            
        ##获取conditions依赖的key
        with connection.cursor() as cursor:        
            conditions_sql ="""
            select distinct conditions from  dbpmsadm.pd_cfgtemplateinterface_rel where template_id =:tempOrInter
            and conditions is not null
            """
            result = cursor.execute(conditions_sql,{'tempOrInter':tempOrInter})
            rows = cursor.fetchall()  # 得到所有数据集
            for s in rows:
                dependence_kyes = dependence_kyes|self.findall_keys(s[0])


        ## 获取sys_check依赖的key
        with connection.cursor() as cursor:        
            sys_check_sql = """
            select distinct sys_check from dbpmsadm.pd_cfgelement_dict where element_key in (
            select key from  dbpmsadm.pd_cfginterfacestep_rel where interface_id in (
            select interface_id from  dbpmsadm.pd_cfgtemplateinterface_rel where template_id =:tempOrInter
            union
            select interface_id from  dbpmsadm.pd_cfginterface_dict where interface_id  =:tempOrInter

            )
            )and sys_check is not null
           """
            result = cursor.execute(sys_check_sql,{'tempOrInter':tempOrInter})
            rows = cursor.fetchall()  # 得到所有数据集
            for s in rows:
                dependence_kyes = dependence_kyes|self.findall_keys(s[0])

                
            
        ##获取data_type = S 的key
        with connection.cursor() as cursor:        
            data_s_sql = """
            select data_key from dbpmsadm.pd_cfgdata_dict where data_key in (
            select key from  dbpmsadm.pd_cfginterfacestep_rel where interface_id in (
            select interface_id from  dbpmsadm.pd_cfgtemplateinterface_rel where template_id =:tempOrInter
            union
            select interface_id from  dbpmsadm.pd_cfginterface_dict where interface_id  =:tempOrInter

            )
            )
            """

            result = cursor.execute(data_s_sql,{'tempOrInter':tempOrInter})
            rows = cursor.fetchall()  # 得到所有数据集
            for s in rows:
                dependence_kyes.add(s[0])

            #遍历并获取依赖key 
            for key in dependence_kyes:
                dependence_kyes = dependence_kyes|self.get_depnd_keys(connection,key)

        #剔除   pd_cfginterfacestep_rel 中的key,因为这些是单独导出sql的
        #获取 模板关联的pd_cfginterfacestep_rel  中的key列表
        with connection.cursor() as cursor:    
            step_rel_sql = """
            select key from  dbpmsadm.pd_cfginterfacestep_rel where interface_id in (
            select interface_id from  dbpmsadm.pd_cfgtemplateinterface_rel where template_id =:tempOrInter
            union
            select interface_id from  dbpmsadm.pd_cfginterface_dict where interface_id  =:tempOrInter

            )"""            
            result = cursor.execute(step_rel_sql,{'tempOrInter':tempOrInter})
            rows = cursor.fetchall()  # 得到所有数据集
            for k in rows:
                dependence_kyes.discard(k[0])

        print('dependence_kyes============================\n',dependence_kyes)
        return dependence_kyes

            
    """

     ---------------------------
     用于获取模板级脚本
     ---------------------------
    """
    def export_tmp_sql(self,template,connection):
        content = []
        log1 = '----------------注意！注意！---这里的脚本是需要自行到SQLDEV中手动执行导出可执行SQL脚本=begin=========================\n'
        content.append(log1)
        
        pd_cfgtemplate_dict = "select * from  dbpmsadm.pd_cfgtemplate_dict where template_id ='"+template+"';\n"
        content.append(pd_cfgtemplate_dict)
        
        pd_cfgtemplateinterface_rel = "select * from  dbpmsadm.pd_cfgtemplateinterface_rel where template_id ='"+template+"';\n"
        content.append(pd_cfgtemplateinterface_rel)
        
        pd_cfginterface_dict = """select * from  dbpmsadm.pd_cfginterface_dict where interface_id in (
        select interface_id from  dbpmsadm.pd_cfgtemplateinterface_rel where template_id ='"""+template+"""'
        );\n"""
        content.append(pd_cfginterface_dict)
        
        pd_cfginterfacestep_rel = """select * from  dbpmsadm.pd_cfginterfacestep_rel where interface_id in (
        select interface_id from  dbpmsadm.pd_cfgtemplateinterface_rel where template_id ='"""+template+"""'
        );\n"""
        content.append(pd_cfginterfacestep_rel)
        
        pd_cfgelement_dict = """select * from dbpmsadm.pd_cfgelement_dict where element_key in (
        select key from  dbpmsadm.pd_cfginterfacestep_rel where interface_id in (
        select interface_id from  dbpmsadm.pd_cfgtemplateinterface_rel where template_id ='"""+template+"""'
        )
        );\n"""
        content.append(pd_cfgelement_dict)
        content.extend(self.common_export(template,connection))


        return content

    #接口和模板导出脚本的功能逻辑部分
    def common_export(self,tempOrInter,connection):
        content = []
        #获取模板或者接口其他依赖的keys,这里是排除了pd_cfginterfacestep_rel中关联的key
        dependens_keys = self.export_D_sql(tempOrInter, connection)
        d_key_str = "(\n"
        for k, v in enumerate(dependens_keys):
            if k == 0:
                d_key_str += "'" + v + "'"
            else:
                d_key_str += ",\n'" + v + "'"
        d_key_str += ");\n"

        dependens_keys_sql = "select * from dbpmsadm.pd_cfgdata_dict where data_key in " + d_key_str
        content.append(dependens_keys_sql)
        log2 = '-----------------注意！注意！---这里的脚本是需要自行到SQLDEV中手动执行导出可执行SQL脚本==end==============================\n'
        content.append(log2)


        log3 = '--------------------下面的是当前' + tempOrInter + ' 关联的pd_cfgdata_dict中data_type =S 的导出脚本======begin==================\n'
        content.append(log3)

        content.extend(self.export_S_sql(tempOrInter, connection))

        return content

    """
    
    ---------------------------
    用于获取接口级脚本
    ---------------------------
    """


    def export_inter_sql(self, interface_id, connection):
        content = []
        log1 = '----------------注意！注意！---这里的脚本是需要自行到SQLDEV中手动执行导出可执行SQL脚本=begin=========================\n'
        content.append(log1)

        pd_cfginterface_dict = """select * from  dbpmsadm.pd_cfginterface_dict where interface_id = '{0}';\n""".format(interface_id)
        content.append(pd_cfginterface_dict)

        pd_cfginterfacestep_rel = """select * from  dbpmsadm.pd_cfginterfacestep_rel where interface_id = '{0}';\n""".format(interface_id)
        content.append(pd_cfginterfacestep_rel)

        pd_cfgelement_dict = """select * from dbpmsadm.pd_cfgelement_dict where element_key in (
            select key from  dbpmsadm.pd_cfginterfacestep_rel where interface_id = '{0}'
            );\n""".format(interface_id)
        content.append(pd_cfgelement_dict)

        content.extend(self.common_export(interface_id, connection))
        return content

    def export_data_key_sql(self, data_key, connection):
        content =[]
        dependence_kyes = set()
        with connection.cursor() as cursor:
            key_sql = """
            select data_type,content_clob from  dbpmsadm.pd_cfgdata_dict where data_key = :data_key""".format(data_key)
            result = cursor.execute(key_sql,{'data_key':data_key})
            row = cursor.fetchone()  # 得到所有数据集
            data_type =row[0]
            content_clob = row[1]
        if not data_type == 'S':#当前key不是脚本内容key
            dependence_kyes.add(data_key)

        dependence_kyes = dependence_kyes|self.get_depnd_keys(connection,data_key)#获取当前key依赖的所有key
        d_key_str = "(\n"
        for k, v in enumerate(dependence_kyes):
            if k == 0:
                d_key_str += "'" + v + "'"
            else:
                d_key_str += ",\n'" + v + "'"
        d_key_str += ");\n"

        dependens_keys_sql = "select * from dbpmsadm.pd_cfgdata_dict where data_key in " + d_key_str
        content.append(dependens_keys_sql)

        #通过特殊的sql函数导出data_type= S的这种clob字段脚本语句
        if data_type == 'S' and content_clob:
            with connection.cursor() as cursor:
                key_sql = """
                       select exportclobsql(data_key) from dbpmsadm.pd_cfgdata_dict where data_key =:data_key"""
                result = cursor.execute(key_sql, {'data_key': data_key})
                row = cursor.fetchone()  # 得到所有数据集
                content.append(row[0].read())

        return content







if __name__=='__main__':
    db = oracleOperation()
    db.openOracleConn()

    #能运行的无条件查询语句
   # print(db.get_depnd_keys(connection,'s_8111_llb_7_2'))
   #  db.export_tmp_sql('t_hlj_ptllby3030101',connection)
   #  connection.close()
