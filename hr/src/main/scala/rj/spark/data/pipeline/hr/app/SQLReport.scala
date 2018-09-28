package rj.spark.data.pipeline.hr.app

import rj.spark.data.pipeline.hr.context.SqlContext

/*
*   SQl Query Exercises
*
*  Databricks Notebook reference
*  @link https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4445428814682093/105390117682831/3092427392316319/latest.html
*  @author rjain
* */
object SQLReport extends App with SqlContext {


  prepareData

  //1. Write a query and compute average salary (sal) of employees distributed by location (loc). Output shouldn't show any locations which don't have any employees.
  val df_1 = sqlContext.sql("SELECT loc,avg(sal) as avg_sal FROM dept , emp WHERE dept.deptno=emp.deptno GROUP BY loc")
  df_1.show()

  //2. Write a query and compute average salary (sal) of employees located in NEW YORK excluding PRESIDENT
  val df_2 = sqlContext.sql(
    """ SELECT avg(sal) as avg_sal
                          FROM dept , emp
                          WHERE dept.deptno=emp.deptno
                          and loc='NEW YORK'
                          and job!='PRESIDENT'
                          """)
  df_2.show()

  //3. Write a query and compute average salary (sal) of four most recently hired employees
  val df_3 = sqlContext.sql(
    """ SELECT avg(sal) as avg_sal
                             FROM
                              (
                               SELECT sal
                               FROM emp
                               ORDER BY hiredate desc
                              )
                            LIMIT 4
                          """)
  df_3.show()

  //sqlContext.sql("")

  //4. Write a query and compute minimum salary paid for different kinds of jobs in DALLAS
  val df_4 = sqlContext.sql(
    """SELECT job,min(sal) as min_sal
                          FROM dept , emp
                          WHERE dept.deptno=emp.deptno
                          and loc='DALLAS'
                    GROUP BY job
                """)
  df_4.show()


  def prepareData() = {

    //dept table DDL
    sqlContext.sql("DROP TABLE IF EXISTS dept")
    sqlContext.sql(
      """
        |CREATE TABLE IF NOT EXISTS dept (
        |  deptno int,
        |  dname string,
        |  loc string
        |)
      """.stripMargin)

    sqlContext.sql("INSERT INTO TABLE dept  VALUES (10,'ACCOUNTING','NEW YORK')")
    sqlContext.sql("INSERT INTO TABLE dept  VALUES (20,'RESEARCH','DALLAS')")
    sqlContext.sql("INSERT INTO TABLE dept  VALUES (30,'SALES','CHICAGO')")
    sqlContext.sql("INSERT INTO TABLE dept  VALUES (40,'OPERATIONS','BOSTON')")

    //emp table DDL
    sqlContext.sql("DROP TABLE IF EXISTS emp")

    sqlContext.sql(
      """
        |CREATE TABLE IF NOT EXISTS emp (
        |  empno int,
        |  ename string,
        |  job string,
        |  mgr string,
        |  hiredate string,
        |  sal int,
        |  comm string,
        |  deptno int)
      """.stripMargin)

    //emp table DML
    sqlContext.sql("INSERT INTO TABLE emp VALUES (7839,'KING','PRESIDENT','','1981-11-17',5000,'',10)")
    sqlContext.sql("INSERT INTO TABLE emp  VALUES (7698,'BLAKE','MANAGER','7839','1981-01-05',2850,'',30)")
    sqlContext.sql("INSERT INTO TABLE emp VALUES (7782,'CLARK','MANAGER','7839','1981-09-06',2450,'',10)")
    sqlContext.sql("INSERT INTO TABLE emp  VALUES (7566,'JONES','MANAGER','7839','1981-04-02',2975,'',20)")
    sqlContext.sql("INSERT INTO TABLE emp  VALUES (7788,'SCOTT','ANALYST','7566','1987-07-13',3000,'',20)")
    sqlContext.sql("INSERT INTO TABLE emp  VALUES (7902,'FORD','ANALYST','7566','1981-12-03',3000,'',20)")
    sqlContext.sql("INSERT INTO TABLE emp  VALUES (7369,'SMITH','CLERK','7902','1980-12-17',800,'',20)")
    sqlContext.sql("INSERT INTO TABLE emp VALUES (7499,'ALLEN','SALESMAN','7698','1981-02-20',1600,'300',30)")
    sqlContext.sql("INSERT INTO TABLE emp  VALUES (7521,'WARD','SALESMAN','7698','1981-02-22',1250,'500',30)")
    sqlContext.sql("INSERT INTO TABLE emp  VALUES (7654,'MARTIN','SALESMAN','7698','1981-09-28',1250,'1400',30)")
    sqlContext.sql("INSERT INTO TABLE emp  VALUES (7844,'TURNER','SALESMAN','7698','1981-09-08',1500,'0',30)")
    sqlContext.sql("INSERT INTO TABLE emp  VALUES (7876,'ADAMS','CLERK','7788','1987-07-13',1100,'',20)")
    sqlContext.sql("INSERT INTO TABLE emp  VALUES (7900,'JAMES','CLERK','7698','1981-03-12',950,'',30)")
    sqlContext.sql("INSERT INTO TABLE emp  VALUES (7934,'MILLER','CLERK','7782','1982-01-23',1300,'',10)")
  }
}
