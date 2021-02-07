
#' 设置通用的数据库连接
#'
#' @param ip 服务器地址
#' @param port 服务器端口
#' @param user_name  用户名
#' @param password  密码
#' @param db_name  数据库名称
#'
#' @return 返回值
#' @import RJDBC
#' @export
#'
#' @examples
#' sql_conn_common()
sql_conn_common <- function(ip='115.159.201.178',
                            port=1433,
                            user_name='sa',
                            password='Hoolilay889@',
                            db_name='JH_2018B'
){

  drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver","/opt/jdbc/mssql-jdbc-7.2.2.jre8.jar")
  con_str <- paste("jdbc:sqlserver://",ip,":",port,";databaseName=",db_name,sep="");
  con <- dbConnect(drv, con_str, user_name, password)
  return(con)

}
#' 执行sql的查询语句
#'
#' @param conn 连接信息
#' @param sql_str 语句
#'
#' @return 返回值
#' @import RJDBC
#' @export
#'
#' @examples sql_select();
sql_select <- function(conn,sql_str) {
  res <- dbGetQuery(conn,sql_str)
  return(res)
}

# 更新update数据------
#' 更新update数据
#'
#' @param conn 连接信息
#' @param sql_str sql信息
#'
#' @return 返回值
#' @import RJDBC
#' @export
#'
#' @examples sql_update();
sql_update <- function(conn,sql_str) {

  dbSendUpdate(conn,sql_str);

}


#' 将R对象写入到SQL_server表中
#'
#' @param conn 连接信息
#' @param table_name 表名
#' @param r_object R对象表
#' @param skip 是否跳过表名重复检查，默认为是，不需要重复性
#'
#' @return 返回T表示插入成功，F可能是表名重复等
#' @import RJDBC
#' @export
#'
#' @examples db_saveR2Table();
sql_writeTable <- function(conn,table_name,r_object,append=FALSE){

  if (append == FALSE){
    res<- dbWriteTable(conn, table_name, r_object)
  }else{
    res<- dbWriteTable(conn, table_name, r_object,append=T, row.names=F, overwrite=F)
  }

  return(res)

}


#' 分页查询
#'
#' @param sql_str 基本语句
#' @param page_by 分页字段
#' @param from 开始
#' @param to  结束
#' @param conn  连接信息
#'
#' @return 返回值
#' @import RJDBC
#' @export
#'
#' @examples()
#' hana_select_paging
sql_select_paging <- function(conn,
                               sql_str,
                               page_by='fid',
                               from=1,
                               to =10000
) {

  from <- as.character(from)
  to <- as.character(to)

  sql_str2 <- paste0(sql_str," where ",page_by," >= ",from,"  and ",page_by," <=  ",to)

  print(sql_str2)
  res <- dbGetQuery(conn,sql_str2)
  return(res)



}




#' 返回表的行数
#'
#' @param table_name  表名
#' @param conn  连接
#'
#' @return 返回值
#' @import RJDBC
#' @export
#'
#' @examples
#' hana_tableCount()
sql_tableCount <- function(conn,table_name) {

  sql <- paste0("select  count(1)  as FCount   from  ",table_name)
  r<- sql_select(conn,sql_str = sql)
  res <- r$FCount
  return(res)

}
