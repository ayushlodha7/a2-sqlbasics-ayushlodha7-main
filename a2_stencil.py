import pymysql
pymysql.install_as_MySQLdb()

import MySQLdb

class DbStreamer:

    @staticmethod
    def get_rows(data):
        data_rows = []
        for row in data:
            data_rows.append(row)
        return data_rows


    def __init__(self, host, user, password, database):
        self.conn = MySQLdb.Connection(host=host,
                                       user=user,
                                       passwd=password,
                                       db=database,
                                       charset="utf8",
                                       use_unicode=True)
        _cursor = self.conn.cursor()
        return

    def get_connection(self):
        return self.conn

    def close_connection(self):
        self.conn.commit()
        self.conn.close()
        return
    
    def get_tables(self):
        sql = "SHOW TABLES;"
        
        _cursor = self.conn.cursor()
        _cursor.execute(sql)
        data = _cursor.fetchall()

        return data

    def q0(self):
        sql = "SELECT DATE('2020-01-23');"
        _cursor = self.conn.cursor()
        _cursor.execute(sql)
        data = _cursor.fetchall()
        return data

    # TODO: Add your logic for each of the questions in the corresponding methods provided below. Each method should return a list of tuples/rows without the header.
    def q1(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        # sql = "SELECT * from users WHERE vote_count =11800;"
        sql = "SELECT title FROM movie_details_al ORDER BY vote_count DESC LIMIT 5;"
        _cursor.execute(sql)
        data = _cursor.fetchall()
        return data


        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q2(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "SELECT movieid FROM movie_details_al ORDER BY budget DESC LIMIT 1;"
        _cursor.execute(sql)
        data = _cursor.fetchall()
        return data


        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q3(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "SELECT title, runtime FROM movie_details_al WHERE runtime = (SELECT MIN(runtime) FROM movie_details_al) or runtime = (SELECT MAX(runtime) FROM movie_details_al) ;"
        _cursor.execute(sql)
        data = _cursor.fetchall()
        return data


        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q4(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "SELECT movieid, release_date FROM movie_details_al WHERE release_date = (SELECT MIN(release_date) FROM movie_details_al) or release_date = (SELECT MAX(release_date) FROM movie_details_al) ;"
        _cursor.execute(sql)
        data = _cursor.fetchall()
        return data

        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q5(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "SELECT title,budget FROM movie_details_al WHERE popularity = (SELECT MAX(popularity) FROM movie_details_al);"
        _cursor.execute(sql)
        data = _cursor.fetchall()
        return data


        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q6(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "SELECT JSON_EXTRACT(production_companies, '$**.id') AS 'prod_company_id' FROM original_csv_al WHERE popularity = (SELECT MAX(popularity) FROM original_csv_al);"
        _cursor.execute(sql)
        data = _cursor.fetchall()
        return data


        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q7(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "SELECT COUNT(*) as 'num_rows' FROM original_csv_al WHERE JSON_LENGTH(production_companies, '$**.id') >2;"
        _cursor.execute(sql)
        data = _cursor.fetchall()
        return data


        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q8(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # -----------------------------------------------------------------------------------
        sql = "SELECT title FROM original_csv_al ORDER BY (revenue-budget) DESC LIMIT 1"
        _cursor.execute(sql)
        data = _cursor.fetchall()
        return data

        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q9(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "SELECT title, JSON_EXTRACT(genres, '$**.name') FROM original_csv_al WHERE JSON_LENGTH(genres, '$**.id') = 7;"
        # sql = "SELECT movieid, COUNT(movieid) AS `cnt` FROM movieid_genreid_al GROUP BY movieid ORDER BY `cnt` DESC LIMIT 4"
        _cursor.execute(sql)
        data = _cursor.fetchall()
        return data
        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

    def q10(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql =   "SELECT prod_id FROM prod_id_avg_vote_al GROUP BY prod_id ORDER BY COUNT(prod_id) DESC LIMIT 1;"
        _cursor.execute(sql)
        data = _cursor.fetchall()
        return data
        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

    def q11(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "(SELECT DISTINCT JSON_EXTRACT(production_companies, '$**.id') FROM original_csv_al ORDER BY vote_count DESC LIMIT 5)"
        _cursor.execute(sql)
        data = _cursor.fetchall()
        return data
        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q12(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "SELECT DISTINCT prod_id FROM pavgvote WHERE avg_vote>7 AND prod_id NOT IN (SELECT DISTINCT prod_id FROM pavgvote WHERE avg_vote<7);"
        # sql = "SELECT prod_id FROM pavgvote WHERE avg_vote>7 (SELECT prod_id FROM pavgvote WHERE avg_vote<7);"
        _cursor.execute(sql)
        data = _cursor.fetchall()
        return data
        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q13(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "(SELECT title FROM original_csv_al WHERE JSON_CONTAINS(keywords,'818') ORDER BY popularity DESC LIMIT 5) UNION (SELECT title FROM original_csv_al WHERE JSON_CONTAINS(keywords,'818') ORDER BY popularity LIMIT 5);"
        _cursor.execute(sql)
        data = _cursor.fetchall()
        return data
        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q14(self):
        _cursor = self.conn.cursor()

        #doubt
        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "SELECT g_name, AVG(votecount) FROM genre_vote_csv_al GROUP BY g_name"
        _cursor.execute(sql)
        data = _cursor.fetchall()
        return data
        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q15(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "SELECT title, JSON_EXTRACT(genres, '$**.name') AS 'genre_name' FROM original_csv_al ORDER BY budget DESC LIMIT 6"
        _cursor.execute(sql)
        data = _cursor.fetchall()
        return data


        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data


if __name__ == "__main__":
    ## ToDO: Init the DbStreamer object
    db_streamer = DbStreamer(host='localhost',user='root',password='Ayush&&&123',database="movies")

    print("---------------------------------Q0-test-------------------------------------------")
    print(db_streamer.q0())
    # print(db_streamer.get_tables())    
    print("---------------------------------Q1-------------------------------------------")
    print(db_streamer.q1())
    print("---------------------------------Q2-------------------------------------------")
    print(db_streamer.q2())
    print("---------------------------------Q3-------------------------------------------")
    print(db_streamer.q3())
    print("---------------------------------Q4-------------------------------------------")
    print(db_streamer.q4())
    print("---------------------------------Q5-------------------------------------------")
    print(db_streamer.q5())
    print("---------------------------------Q6-------------------------------------------")
    print(db_streamer.q6())
    print("---------------------------------Q7-------------------------------------------")
    print(db_streamer.q7())    
    print("---------------------------------Q8-------------------------------------------")
    print(db_streamer.q8())      
    print("---------------------------------Q9-------------------------------------------")
    print(db_streamer.q9())
    print("---------------------------------Q10-------------------------------------------")
    print(db_streamer.q10())
    print("---------------------------------Q11-------------------------------------------")
    print(db_streamer.q11())
    print("---------------------------------Q12-------------------------------------------")
    print(db_streamer.q12())
    print("---------------------------------Q13-------------------------------------------")
    print(db_streamer.q13())
    print("---------------------------------Q14-------------------------------------------")
    print(db_streamer.q14())
    print("---------------------------------Q15-------------------------------------------")
    print(db_streamer.q15())