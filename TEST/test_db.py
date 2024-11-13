from DB.data_extraction import data_extract,connect_db,close_db
# from DB.data_extraction import connect_db,close_db
import pytest

def test_connecting_to_database():
    result=data_extract()
    assert result

def test_connection_fails_when_closed():
    conn=connect_db()
    close_db(conn)
    with pytest.raises(Exception) as e:
        conn.run("SELECT * from sales_order")
    assert "closed" in str(e)

def test_datetype_of_result():
    result=data_extract()
    assert type(result)==list
    assert type(result[0])==list

    


