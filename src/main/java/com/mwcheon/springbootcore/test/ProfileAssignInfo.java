package com.mwcheon.springbootcore.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static common.system.LoadBalancerEncoderBase.getColumn;

public class ProfileAssignInfo {
	private static Logger logger = LoggerFactory.getLogger(ProfileAssignInfo.class);
	
	public static class ProfileInfo{
		public String strDivision;
		public String type;
		public String resolution;
	}
	
	public static int getProfileAssign( String proName )
	{
		int result = 0;
		
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		String sqlParam = null;
		long pid = -1;
		String type = null;
		String resolution = null;
		String strDivision = null; 
						
		try {
			conn = db.startAutoCommit();
			String sql = null;
			
			sql = "SELECT * FROM cems.profile WHERE cems.profile.name=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setString(1, proName);
				try( ResultSet rs = pstmt.executeQuery() ){
					if( rs.next() ){
						type = rs.getString("type");
						resolution = rs.getString("resolution");
						strDivision = rs.getString("division");
					}
				}
			}
						
			result = getAssign(type, resolution, strDivision);
		
			return result;
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			db.close();
		}
		
		return -1;
	}
	
	public static int getIdAssign( String tld, String cid ) throws Exception
	{
		int result = 0;
		
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		String sqlParam = null;
		long pid = -1;
		String type = null;
		String resolution = null;
		String strDivision = null;
						
		try {
			conn = db.startAutoCommit();
			String sql = null;
			
			sql = "SELECT profile_id FROM cems." + tld + " WHERE cid=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setString(1, cid);
				try( ResultSet rs = pstmt.executeQuery() ){
					if( rs.next() ){
						long tempPid = rs.getLong("profile_id");
						if( rs.wasNull() == false ){
							pid = tempPid;								
						}
					}
				}
			}
			
			if( pid == -1 ){
				throw new EmsException(ErrorCode.INVALID_VALUE_PROPERTY, String.format("[ %s ]Profile id get fail | [cid : %s] ", tld, cid) );
			}
									
			sql = "SELECT * FROM cems.system WHERE cems.system.id=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setLong(1, pid);				
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {			
						type = rs.getString("type");
						resolution = rs.getString("resolution");
						strDivision = rs.getString("division");
					}
				}
			}					
			
			result = getAssign(type, resolution, strDivision);
		
			return result;
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			db.close();
		}
		
		return -1;
	}
	
	//프로파일 정보 조회
	public static ProfileInfo getProfileType( long pid )
	{
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		try {
			conn = db.startAutoCommit();
			String sql = null;
						
			sql = "SELECT * FROM cems.profile WHERE cems.profile.id=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setLong(1, pid);
				logger.debug(pstmt.toString());
				try( ResultSet rs = pstmt.executeQuery() ){				
					if (rs.next()) {
						ProfileInfo info = new ProfileInfo();
						info.type = rs.getString("type");
						info.resolution = rs.getString("resolution");
						info.strDivision = rs.getString("division");
						
						return info;
					}
				}
			}												
			
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			db.close();
		}
		
		return null;
	}
	
	//프로파일 정보 조회
	public static ProfileInfo getProfileType( Connection conn, long pid ) throws Exception
	{			
		ProfileInfo info = null;
		
		try {		
			String sql = null;
						
			sql = "SELECT * FROM cems.profile WHERE cems.profile.id=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setLong(1, pid);
				logger.debug(pstmt.toString());
				try( ResultSet rs = pstmt.executeQuery() ){				
					if (rs.next()) {
						info = new ProfileInfo();
						info.type = rs.getString("type");
						info.resolution = rs.getString("resolution");	
						info.strDivision = rs.getString("division");
					}
				}
			}												
			
		}finally{
			
		}
		
		return info;
	}
	
	public static Long getProfileID( String tbl, String id )
	{
		
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		try {
			conn = db.startAutoCommit();
			String sql = null;
						
			sql = String.format("SELECT * FROM cems.%s WHERE cems.%s.cid='%s'", tbl, tbl, id);
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {				
				logger.debug(pstmt.toString());
				try( ResultSet rs = pstmt.executeQuery() ){				
					if (rs.next()) {
						long pid = rs.getLong("profile_id");
						if( rs.wasNull() == false ){
							return pid;
						}else{
							return null;
						}								
					}
				}
			}												
			
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			db.close();
		}
		
		return null;
	}
	
	public static Long getProfileID( Connection conn, String tbl, String id ) throws Exception
	{		
		try {	
			String sql = null;
						
			sql = String.format("SELECT * FROM cems.%s WHERE cems.%s.cid='%s'", tbl, tbl, id);
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {				
				logger.debug(pstmt.toString());
				try( ResultSet rs = pstmt.executeQuery() ){				
					if (rs.next()) {
						long pid = rs.getLong("profile_id");
						if( rs.wasNull() == false ){
							return pid;
						}else{
							return null;
						}					
					}
				}
			}												
			
		}finally
		{
			
		}
		
		return null;
	}
	
	//프로파일의 작업 할당량 구하기
	public static int getAssign( String proType, String proResolution, String strDivision ) throws Exception
	{	
		int result = 0;
		
		DbTransaction db = null;
		Connection conn = null;
		
		try {
			if( strDivision != null && strDivision.toUpperCase().equals("SUPERNOVA") ) {
				int nDefineMaxAssign = 1300;
				
				String strMaxAssign = new DbCommonHelper().GetConfigureDBValue(null, "enc_max_assign");
				if( strMaxAssign != null && Util.isNumber( strMaxAssign )) {
					nDefineMaxAssign = Integer.valueOf( strMaxAssign );
				}
				
				result = nDefineMaxAssign;
				
			}else {
				db = new DbTransaction();
				conn = db.startAutoCommit();
				try {
					String sql = "SELECT cems.configure.value FROM cems.configure WHERE cems.configure.key=?";
					try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
						pstmt.setString(1, getKeyString(proType, proResolution));
						try( ResultSet rs = pstmt.executeQuery() ){
							if (rs.next()) {			
								if( rs.getString(1) != null ){
									result = Integer.parseInt(rs.getString(1));
								}															
							}
						}
					}	
				}catch( Exception ex ) {
					logger.error("",ex);
					throw ex;
				}finally {
					db.close();
				}					
			}
			
		} catch (Exception e) {
			logger.error("", e);
		}
		
		return result;
	}

	public static int getAssign( String proType, String proResolution, String strDivision, long sid ) throws Exception
	{
		logger.info(String.format("get Assign | proType[%s] | proResolution [%s] | strDivision [%s] | sid [%s]",
				proType, proResolution, strDivision, sid));
		int result = 0;

		DbTransaction db = null;
		Connection conn = null;

		try {
			db = new DbTransaction();
			conn = db.startAutoCommit();
			try {

				String targetColumn = getColumn(proType, proResolution);
				logger.info("target Column : "+targetColumn);
				String selSysSql = "SELECT * FROM cems.system WHERE vendor = '" + strDivision + "' and id = ?";
				try (PreparedStatement pstmt = conn.prepareStatement(selSysSql)) {
					pstmt.setString(1, String.valueOf(sid));
					try( ResultSet rs = pstmt.executeQuery() ){
						if (rs.next()) {
							if( rs.getString(1) != null ){

								result = Integer.parseInt(rs.getString(targetColumn));
								logger.info("target Column result: "+ result);

							}
						}
					}
				}
			}catch( Exception ex ) {
				logger.error("",ex);
				throw ex;
			}finally {
				db.close();
			}

		} catch (Exception e) {
			logger.error("", e);
		}

		return result;
	}
	
	//프로파일의 작업 할당량 구하기
	public static int getAssignEx( Connection conn, String proType, String proResolution, String strDivision ) throws Exception
	{	
		int result = 0;
		
		if( strDivision != null && strDivision.toUpperCase().equals("SUPERNOVA") ) {
			int nDefineMaxAssign = 1300;
			
			String strMaxAssign = new DbCommonHelper().GetConfigureDBValue(null, "enc_max_assign");
			if( strMaxAssign != null && Util.isNumber( strMaxAssign )) {
				nDefineMaxAssign = Integer.valueOf( strMaxAssign );
			}
			
			result = nDefineMaxAssign;
			
		}else {				
			try {
				String sql = "SELECT cems.configure.value FROM cems.configure WHERE cems.configure.key=?";
				try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
					pstmt.setString(1, getKeyString(proType, proResolution));
					try( ResultSet rs = pstmt.executeQuery() ){
						if (rs.next()) {			
							if( rs.getString(1) != null ){
								result = Integer.parseInt(rs.getString(1));
							}															
						}
					}
				}	
			}catch( Exception ex ) {
				logger.error("",ex);
				throw ex;
			}					
		}
	
		return result;
	}

	public static int getEncoderAssignEx(Connection conn, String proType, String proResolution, String strDivision, long lSid) throws Exception {
		int result = 0;

		try {
			String column = getColumn(proType, proResolution);

			String sql = "SELECT * FROM cems.system where id = ?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setString(1, String.valueOf(lSid));
				try (ResultSet rs = pstmt.executeQuery()) {
					if (rs.next()) {
						if (rs.getString(1) != null) {

							if (strDivision != null && strDivision.toUpperCase().equals("SUPERNOVA")) {
								result = Integer.parseInt(rs.getString("enc_max_assign"));
							} else {
								result = Integer.parseInt(rs.getString(column));
							}
						}
					}
				}
			}
		} catch (Exception ex) {
			logger.error("", ex);
			throw ex;
		}

		return result;
	}
	
	//프로파일의 작업 할당량 구하기
	public static int getAssign( Connection conn, String proType, String proResolution ) throws Exception
	{	
		int result = -1;
					
		String sqlParam = null;
				
		try {			
			String sql = null;
			
			sql = "SELECT cems.configure.value FROM cems.configure WHERE cems.configure.key=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setString(1, getKeyString(proType, proResolution));
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {			
						if( rs.getString(1) != null ){
							result = Integer.parseInt(rs.getString(1));
						}															
					} 
				}
			}																	
		} finally {
			
		}
		
		if (result == -1) {
			logger.error("no value (" + proType + "," + proResolution + ") of configure.key");
		}
		
		return result;
	}
	
	public static int getAssign( Connection paramConn, String proType, String proResolution, String strDivision ) throws Exception
	{	
		int result = -1;
					
		String sqlParam = null;
				
		DbTransaction db = null;
		Connection conn = null;
		
		try {	
			
			if( paramConn == null ) {
				db = new DbTransaction();
				conn = db.startAutoCommit();
			}else {
				conn = paramConn;
			}
								
			String sql = "SELECT cems.configure.value FROM cems.configure WHERE cems.configure.key=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				if( strDivision != null && strDivision.toUpperCase().equals("SUPERNOVA") ) {
					pstmt.setString(1, getKeyStringBySuperNova(proType, proResolution) );
				}else {
					pstmt.setString(1, getKeyString(proType, proResolution) );
				}
				
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {			
						if( rs.getString(1) != null ){
							result = Integer.parseInt(rs.getString(1));
						}															
					} 
				}
			}finally {
				if( db != null ) {
					db.close();
					db = null;
				}
			}
		}catch( Exception ex ){
			logger.error("",ex);
			throw ex;
		}
		
		if (result == -1) {
			logger.error("no value (" + proType + "," + proResolution + ") of configure.key");
		}
		
		return result;
	}

	public static int getEncoderAssign( Connection paramConn, String proType, String proResolution, String strDivision, long lSid ) throws Exception
	{
		int result = -1;

		DbTransaction db = null;
		Connection conn = null;

		try {

			if( paramConn == null ) {
				db = new DbTransaction();
				conn = db.startAutoCommit();
			}else {
				conn = paramConn;
			}

			String sql = "SELECT * FROM cems.system where id = ?";

			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setString(1, String.valueOf(lSid));
				String column = getColumn(proType, proResolution);
				logger.info(String.format("getEncoderAssign sql = [%s]" , sql));
				logger.info(String.format("column = [%s]" , column));

				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {
						if( rs.getString(1) != null ){
							result = Integer.parseInt(rs.getString(column));
							logger.info(String.format("result = [%d]" , result));

						}
					}
				}
			}finally {
				if( db != null ) {
					db.close();
					db = null;
				}
			}
		}catch( Exception ex ){
			logger.error("",ex);
			throw ex;
		}

		if (result == -1) {
			logger.error("no value (" + proType + "," + proResolution + ") of configure.key");
		}

		return result;
	}
	
	private static String getKeyString( String proType, String proResolution ) throws Exception
	{
		String result = null;
		
		if( proType == null || proType.length() <= 0 ){
			throw new EmsException( ErrorCode.INVALID_VALUE_PROPERTY, "profile type param is null" );
		}
		
		if( proResolution == null || proResolution.length() <= 0 ){
			throw new EmsException( ErrorCode.INVALID_VALUE_PROPERTY, "profile Resolution param is null" );
		}
		
		proResolution = proResolution.toLowerCase();
		proType = proType.toLowerCase();
		if( proType.equals("tv") || proType.equals("cug")){
			result = "tv_" + proResolution + "_assign";
		}else if( proType.equals("mobile") ){
			result = "mobile_" + proResolution + "_assign";
		}else if( proType.equals("clip") ){
			result = "cl_" + proResolution + "_assign";
		}
		
		return result;
	}
	
	private static String getKeyStringBySuperNova( String proType, String proResolution ) throws Exception
	{
		String result = null;
		
		if( proType == null || proType.length() <= 0 ){
			throw new EmsException( ErrorCode.INVALID_VALUE_PROPERTY, "profile type param is null" );
		}
		
		if( proResolution == null || proResolution.length() <= 0 ){
			throw new EmsException( ErrorCode.INVALID_VALUE_PROPERTY, "profile Resolution param is null" );
		}
		
		proResolution = proResolution.toLowerCase();
		proType = proType.toLowerCase();
		if( proType.equals("tv") || proType.equals("cug")){
			result = "opt_sn_tv_" + proResolution + "_assign";
		}else if( proType.equals("mobile") ){
			result = "opt_sn_mobile_" + proResolution + "_assign";
		}else if( proType.equals("clip") ){
			result = "opt_sn_cl_" + proResolution + "_assign";
		}
		
		return result;
	}
		
}
