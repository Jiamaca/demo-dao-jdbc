package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.mysql.jdbc.Statement;

import db.DB;
import db.DbException;
import model.dao.DepartmentDao;
import model.entities.Department;

public class DepartmentDaoJDBC implements DepartmentDao {
	
	private Connection conn;
	
    public DepartmentDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Department obj) {
		PreparedStatement st = null;
		
		try {
			st = conn.prepareStatement("INSERT INTO Department"
					+ "(Name)"
					+ "VALUES"
					+ "(?)", Statement.RETURN_GENERATED_KEYS);
			
			st.setString(1, obj.getName());
			
			int rows = st.executeUpdate();
			
			if (rows > 0) {
				ResultSet rs = st.getGeneratedKeys();
				if (rs.next()) {
					int id = rs.getInt(1);
					obj.setId(id);
				}
			}
			else
				throw new DbException("Erro inesperado! Nenhuma linha foi afetada");
		}  catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
		
	}

	@Override
	public void deleteById(Integer id) {
		PreparedStatement st = null;
		
		try {
			st = conn.prepareStatement(" DELETE FROM Department "
					+ "	WHERE Id = ?");
		
			st.setInt(1, id);
			
			st.executeUpdate();
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
		
	}

	@Override
	public Department findById(Integer id) {
		PreparedStatement st = null;
		
		try {
			st = conn.prepareStatement("SELECT * FROM Department "
					+ " WHERE Id = ? "
					+ " ORDER BY Name ");
			
			st.setInt(1, id);
			
			ResultSet rs = st.executeQuery();
			
			if (rs.next()) {
				return instantiateDeparment(rs);
			}
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
			
		} finally {
			DB.closeStatement(st);
		}
		
	
		
		return null;
	}

	@Override
	public List<Department> findAll() {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement(" SELECT * FROM department "
					+ " ORDER BY Name");
			
			ResultSet rs = st.executeQuery();
			
			List<Department> deps = new ArrayList<>();
			
			while (rs.next()) {
				deps.add(instantiateDeparment(rs));
			}
			
			return deps;
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
		
	}
   
	@Override
	public void update(Department dep) {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("UPDATE Department"
					+ " SET Name = ? "
					+ " WHERE Id = ? ");
			
			st.setString(1, dep.getName());
			st.setInt(2, dep.getId());
			
			st.executeUpdate();
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}
	
	public Department instantiateDeparment(ResultSet rs) throws SQLException {
		Department dep = new Department();
		dep.setId(rs.getInt("Id"));
		dep.setName(rs.getString("Name"));
		return dep;
	
	}
}

