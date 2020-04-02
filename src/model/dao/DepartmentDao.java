package model.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import model.entities.Department;

public interface DepartmentDao {

	void insert(Department obj);
	void deleteById(Integer id);
	Department findById(Integer id);
	List<Department> findAll();
	Department instantiateDeparment(ResultSet rs) throws SQLException;
	void update(Department dep);
}
