package com.mwcheon.springbootcore.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import common.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadBalancerEncoderBase {
	private static Logger logger = LoggerFactory.getLogger(LoadBalancerEncoderBase.class);
	private String eamsHost = null;
	//public static final int maxAssign = 1000;
	
	//====================================================================
	// 메모리 상으로 인코더 내용을 관리 시 이중화 된 EMS 서버랑 싱크가 맞지 않을 수 있음
	//====================================================================
	private static class InitItem{
		public long sid;
		public int totalAssign;
	}
	
	public enum EncVendor
	{
		GALAIXA(1),
		ELEMENTAL(2),
		SUPERNOVA(3);
		
		final int val;
		EncVendor(int val) {
			this.val = val;
		}

		public int get(){
			return val;
		}
	}
		
	public static void initBalnacer()
	{
		// 1. DB에 있는 인코더 Assign을 0으로 초기화		
		String eamsHost = null;
		
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		
		ArrayList<InitItem> arrayInit = new ArrayList<>();
		
		try{
			
			conn = db.start();
			String sql = null;
			
			// 0으로 초기화
			try{
				sql = "UPDATE cems.system SET cems.system.assign = 0 WHERE cems.system.type='Encoder' AND ( vendor='ELEMENTAL' or vendor='SUPERNOVA' ) ";
				try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
					pstmt.executeUpdate();
				}
				
				db.commit();
			}catch(Exception e){
				logger.error("",e);
				db.rollback();
			} finally {
				db.close();
			}
				
		}catch( Exception e ){
			logger.error("",e);			
		}
		
	}
	
	public static int getSystemEnableCnt( Connection conn, LoadBalancerEncoderBase.EncVendor eEncVendor, boolean bIgnoreUHD, boolean bIgnoreWatermark ) throws Exception
	{
		int nRet = 0;
			
		String  sql = "SELECT count(*) FROM cems.system WHERE status=1 AND auto=1 AND cems.system.type='Encoder' AND vendor=?";
		
		if( !bIgnoreUHD ) {
			sql += " AND uhd_use=1";
		}
		
		if( !bIgnoreWatermark ) {
			sql += " AND use_watermark=1";
		}
				
		try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
			pstmt.setString(1, eEncVendor.toString() );
			try( ResultSet rs = pstmt.executeQuery() ){
				if( rs.next()) {
					nRet = rs.getInt(1);
				}
			}
		}
		
		return nRet;		
	}
	
	public static int getSystemEnableCntSuperNova( Connection conn, boolean bIgnoreUHD ) throws Exception
	{
		int nRet = 0;
			
		String  sql = "SELECT count(*) FROM cems.system WHERE status=1 AND auto=1 AND cems.system.type='Encoder' AND vendor='SUPERNOVA'";
		
		if( !bIgnoreUHD ) {
			sql += " AND uhd_use=1";
		}
		
		try (PreparedStatement pstmt = conn.prepareStatement(sql)) {			
			try( ResultSet rs = pstmt.executeQuery() ){
				if( rs.next()) {
					nRet = rs.getInt(1);
				}
			}
		}
		
		return nRet;		
	}
	
		
	public static SystemItem getSystemTbl( long sid ) throws Exception
	{
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		try {
			conn = db.startAutoCommit();
			String sql = null;
			
			sql = "SELECT * FROM cems.system WHERE cems.system.id=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setLong(1, sid);
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {
						SystemItem item = new SystemItem();
						item.id = rs.getLong("id");
						item.ip = rs.getString("ip");
						item.type = rs.getString("type");
						item.usageType = rs.getString("div_usage");
						item.name = rs.getString("name");
						item.capMax = rs.getInt("cap_max");
						item.userid = rs.getString("sys_id");
						item.userpw = rs.getString("sys_pw");
						item.originPath = rs.getString("origin_path");
						item.outputPath = rs.getString("output_path");
						item.assign = rs.getInt("assign");
						item.strAuthorizeID = rs.getString("authorize_id");
						item.strAuthorizePW = rs.getString("authorize_pw");
						item.strPortApi = rs.getString("port_api");
						
						return item;
					}
				}
			}																	
		} finally {
			db.close();
		}
		
		return null;		
	}
	
	public static HashMap<Long, SystemItem> getSystemTbl( ) throws Exception
	{
		HashMap<Long, SystemItem> result = new HashMap<>();
		
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		try {
			conn = db.startAutoCommit();
			String sql = null;
			
			sql = "SELECT * FROM cems.system WHERE cems.system.type='Encoder' AND ( vendor='ELEMENTAL' or vendor='SUPERNOVA' )";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {				
				try( ResultSet rs = pstmt.executeQuery() ){
					while (rs.next()) {
						SystemItem item = new SystemItem();
						item.id = rs.getLong("id");
						item.ip = rs.getString("ip");
						item.type = rs.getString("type");
						item.usageType = rs.getString("div_usage");
						item.name = rs.getString("name");
						item.capMax = rs.getInt("cap_max");
						item.userid = rs.getString("sys_id");
						item.userpw = rs.getString("sys_pw");
						item.originPath = rs.getString("origin_path");
						item.outputPath = rs.getString("output_path");
						item.assign = rs.getInt("assign");
						item.strAuthorizeID = rs.getString("authorize_id");
						item.strAuthorizePW = rs.getString("authorize_pw");
						item.strPortApi = rs.getString("port_api");
						
						result.put(rs.getLong("id"), item);
					}
				}
			}
						
		} finally {
			db.close();
		}
		
		return result;		
	}
	

	public static HashMap<Long, SystemItem> getSystemTbl( Connection conn ) throws Exception
	{
		HashMap<Long, SystemItem> result = new HashMap<>();
		
		try {

			String sql = null;
			
			sql = "SELECT * FROM cems.system WHERE cems.system.type='Encoder' AND ( vendor='ELEMENTAL' or vendor='SUPERNOVA' )";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {				
				try( ResultSet rs = pstmt.executeQuery() ){
					while (rs.next()) {
						SystemItem item = new SystemItem();
						item.id = rs.getLong("id");
						item.ip = rs.getString("ip");
						item.type = rs.getString("type");
						item.name = rs.getString("name");
						item.usageType = rs.getString("div_usage");
						item.capMax = rs.getInt("cap_max");
						item.userid = rs.getString("sys_id");
						item.userpw = rs.getString("sys_pw");
						item.originPath = rs.getString("origin_path");
						item.outputPath = rs.getString("output_path");
						item.assign = rs.getInt("assign");
						item.strAuthorizeID = rs.getString("authorize_id");
						item.strAuthorizePW = rs.getString("authorize_pw");
						
						result.put(rs.getLong("id"), item);
					}
				}
			}
						
		}finally
		{
			
		}
		
		return result;		
	}
	
	public synchronized static void getSystem(long sid, int assign) throws Exception {		
				
		DbTransaction db = new DbTransaction();			
		Connection conn = null;
		
		int cmpAssign = 0;
		int setAssign = 0;
		
		try {		
			conn = db.start();
			String sql = null;
			
			sql = "SELECT cems.system.assign FROM cems.system WHERE cems.system.id=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				int args = 1;				
				pstmt.setLong(args++, sid);
				logger.debug(pstmt.toString());
				try( ResultSet rs =  pstmt.executeQuery() ){
					if( rs.next() ){
						cmpAssign = rs.getInt("assign");
					}
				}
			}
			
			int maxAssign = 1300;
			String strMaxAssign = new DbCommonHelper().GetConfigureDBValue( conn, "enc_max_assign", false );
			if( strMaxAssign != null && common.Util.isNumber( strMaxAssign ) ) {
				maxAssign = Integer.valueOf( strMaxAssign );
			}			
			
			if( ( cmpAssign + assign ) > maxAssign ){
				setAssign = maxAssign;
			}else{
				setAssign = cmpAssign + assign; 
			}
				
			sql = "UPDATE cems.system SET cems.system.assign = ? WHERE cems.system.id=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				int args = 1;
				pstmt.setInt(args++, setAssign);
				pstmt.setLong(args++, sid);
				logger.debug(pstmt.toString());
				pstmt.executeUpdate();									
			}				
			
			db.commit();
		}catch(Exception e){
			db.rollback();
			logger.error("",e );
			throw new EmsException(ErrorCode.DB_ERROR, "getSystem fail");			
		} finally {
			db.close();
		}
	}
	
	public static class TempEncAssignItem
	{
		public long lSid;
		public int nAssign;
		
		public TempEncAssignItem()
		{
			this.lSid = -1;
			this.nAssign = 0;
		}
	}
	
	public synchronized static void returnIdle(long sid, int assign) throws InterruptedException {
		
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		
		int cmpAssign = 0;
		int setAssign = 0;
		try {
			conn = db.start();
			String sql = null;
			
			sql = "SELECT cems.system.assign FROM cems.system WHERE cems.system.id=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				int args = 1;				
				pstmt.setLong(args++, sid);
				logger.debug(pstmt.toString());
				try( ResultSet rs =  pstmt.executeQuery() ){
					if( rs.next() ){
						cmpAssign = rs.getInt("assign");
					}
				}
			}
			
			if( (cmpAssign - assign) < 0 ){
				setAssign = 0;
			}else{
				setAssign = cmpAssign - assign;
			}
			
			sql = "UPDATE cems.system SET cems.system.assign = ? WHERE cems.system.id=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				int args = 1;
				pstmt.setInt(args++, setAssign);
				pstmt.setLong(args++, sid);
				logger.debug(pstmt.toString());
				pstmt.executeUpdate();									
			}
			
			db.commit();
		}catch(Exception e){
			db.rollback();			
			logger.error("",e);			
			throw new InterruptedException();
		} finally {
			db.close();
		}
	}
	
	public static int GetAssignCur( Connection paramConn, long sid) {
		
		DbTransaction db = null;
		Connection conn = null;
		
		int nRet = -1;
		String sql = null;
		
		try {
			if( paramConn == null ) {
				db = new DbTransaction();
				conn = db.start();	
			}else {
				conn = paramConn;
			}
			
			sql = "SELECT cems.system.assign FROM cems.system WHERE cems.system.id=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				int args = 1;				
				pstmt.setLong(args++, sid);
				logger.info(pstmt.toString());
				try( ResultSet rs =  pstmt.executeQuery() ){
					if( rs.next() ){
						nRet = rs.getInt("assign");
					}
				}
			}
						
		}catch(Exception e){					
			logger.error("",e);						
		} finally {
			if( paramConn == null ) {
				db.close();	
			}			
		}
		
		return nRet;
	}
	
	public synchronized static void returnIdle( Connection conn, long sid, int assign) throws Exception {
				
		try {			
			String sql = null;
			int cmpAssign = 0;
			int setAssign = 0;
			
			sql = "SELECT cems.system.assign FROM cems.system WHERE cems.system.id=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				int args = 1;				
				pstmt.setLong(args++, sid);
				logger.debug(pstmt.toString());
				try( ResultSet rs =  pstmt.executeQuery() ){
					if( rs.next() ){
						cmpAssign = rs.getInt("assign");
					}
				}
			}
			
			if( (cmpAssign - assign) < 0 ){
				setAssign = 0;
			}else{
				setAssign = cmpAssign - assign;
			}
			
			sql = "UPDATE cems.system SET cems.system.assign = ? WHERE cems.system.id=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				int args = 1;
				pstmt.setFloat(args++, setAssign);
				pstmt.setLong(args++, sid);
				logger.debug(pstmt.toString());
				pstmt.executeUpdate();									
			}					
		}finally{
			
		}
	}
	
	protected static Boolean nwIOCheck( long sid ) throws Exception
	{						
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		int cap = -1;
		int cnt = 0;
		try {
			conn = db.startAutoCommit();
			String sql = null;
			
			sql = "SELECT cems.configure.value FROM cems.configure WHERE cems.configure.key='cap_nw_io'";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {				
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next() && rs.getString(1) != null ) {										
						cap = Integer.parseInt( rs.getString(1) );										
					}
				}
			}		
			
			if( cap <= 0 ){
				//throw new EmsException(ErrorCode.VALUE_IS_EMPTY, "Encoder Balancer Network Cap is Empty");
				logger.info("Encoder Balancer Network Cap is Empty");
				return false;
			}
			
			// content 와 clip 둘다 eleemental 을 사용하기 때문에 둘다 검사 해야 한다
			sql = String.format("SELECT COUNT(*) FROM cems.content WHERE cems.content.sid=%s and content_status='작업진행' and status!='[원본]다운로드실패' AND status LIKE '[원본]다운로드%%'", sid);
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {				
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next() && rs.getString(1) != null ) {										
						cnt += rs.getInt(1);										
					}
				}
			}
			
			sql = String.format("SELECT COUNT(*) FROM cems.clip WHERE cems.clip.sid=%s AND content_status='작업진행' and status!='[원본]다운로드실패' AND status LIKE '[원본]다운로드%%'", sid);
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {				
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next() && rs.getString(1) != null ) {										
						cnt += rs.getInt(1);										
					}
				}
			}
			
			if( cap >= cnt ){
				return true;
			}else{
				return false;
			}
						
		} finally {
			db.close();
		}				
	}
	
	
	//Encoder System에 할당량을 할당할 수 있는지 조회
	protected static Long getChkSystemAssignEx( ArrayList<SystemItem> systemList , int assign, boolean hasUhd, HashMap< Long, Integer > mapTempEncAssign )
	{
		long result = -1;
		
		int maxAssign = 1300;
		try {
			String strMaxAssign = new DbCommonHelper().GetConfigureDBValue(null, "enc_max_assign");
			if( strMaxAssign != null && common.Util.isNumber( strMaxAssign ) ) {
				maxAssign = Integer.valueOf( strMaxAssign );
			}	
		}catch( Exception ex ) {}				
		
		float beforeSysAssign = maxAssign;
		if( systemList == null || systemList.isEmpty() ){
			logger.debug("Encoder System List empty");
			return result;
		}
		
		for( SystemItem item : systemList ){	
			
			Integer nAlreadyAssign = mapTempEncAssign.get( item.id );
			if( nAlreadyAssign == null ) {
				nAlreadyAssign = 0;
			}
			
			if( ( item.assign + nAlreadyAssign.intValue() ) + assign <= maxAssign ){
				if( hasUhd && item.uhd_use != 1 ){
					continue;
				}
				
				// 전체 인코더 중 가장 할당량이 작은 인코더에 할당
				if( beforeSysAssign > item.assign ){
					beforeSysAssign = item.assign;
					result = item.id;
				}				 
			}
		}
		
		return result;
	}

	protected static Map<String, Object> getChkSystemAssignEx(ArrayList<SystemItem> systemList , boolean hasUhd, HashMap< Long, Integer > mapTempEncAssign )
	{
		Map <String, Object > result = new HashMap<>();
		result.put("sid", -1);

		try {
			for (SystemItem item : systemList) {

				int maxAssign = 1300;
				String strMaxAssign = String.valueOf(item.encMaxAssign);
				if (strMaxAssign != null && common.Util.isNumber(strMaxAssign)) {
					maxAssign = Integer.valueOf(strMaxAssign);
				} else {
					String configureDBValue = new DbCommonHelper().GetConfigureDBValue(null, "enc_max_assign");
					if (configureDBValue != null && common.Util.isNumber(configureDBValue)) {
						maxAssign = Integer.valueOf(configureDBValue);
					}
				}

				float beforeSysAssign = maxAssign;
				if (systemList == null || systemList.isEmpty()) {
					logger.debug("Encoder System List empty");
					return result;
				}


				int nAssign = item.nAssign;
				Integer nAlreadyAssign = mapTempEncAssign.get(item.id);
				if (nAlreadyAssign == null) {
					nAlreadyAssign = 0;
				}

				if ((item.assign + nAlreadyAssign.intValue()) + nAssign <= maxAssign) {
					if (hasUhd && item.uhd_use != 1) {
						continue;
					}

					// 전체 인코더 중 가장 할당량이 작은 인코더에 할당
					if (beforeSysAssign > item.assign) {
						beforeSysAssign = item.assign;
						result.put("sid", item.id);
						result.put("nAssign", item.nAssign);

					}
				}
			}
		}catch (Exception e) {
			logger.error("", e);
		}

		return result;
	}
	
	//Encoder System에 할당량을 할당할 수 있는지 조회
	protected static Long getChkSystemAssign( ArrayList<SystemItem> systemList , int assign, boolean hasUhd )
	{
		long result = -1;
		
		int maxAssign = 1300;
		try {
			String strMaxAssign = new DbCommonHelper().GetConfigureDBValue(null, "enc_max_assign");
			if( strMaxAssign != null && common.Util.isNumber( strMaxAssign ) ) {
				maxAssign = Integer.valueOf( strMaxAssign );
			}	
		}catch( Exception ex ) {}				
		
		float beforeSysAssign = maxAssign;
		if( systemList == null || systemList.isEmpty() ){
			logger.debug("Encoder System List empty");
			return result;
		}
		
		
		for( SystemItem item : systemList ){						
			if( item.assign + assign <= maxAssign ){
				if( hasUhd && item.uhd_use != 1 ){
					continue;
				}
				
				// 전체 인코더 중 가장 할당량이 작은 인코더에 할당
				if( beforeSysAssign > item.assign ){
					beforeSysAssign = item.assign;
					result = item.id;
				}				 
			}
		}
		
		return result;
	}
	
	protected static Boolean getChkSystemAssign( SystemItem systemItem, int assign, boolean hasUhd)
	{
		Boolean result = false;
		
		int maxAssign = 1300;
		try {
			String strMaxAssign = new DbCommonHelper().GetConfigureDBValue(null, "enc_max_assign");
			if( strMaxAssign != null && common.Util.isNumber( strMaxAssign ) ) {
				maxAssign = Integer.valueOf( strMaxAssign );
			}	
		}catch( Exception ex ) {}			

		if( systemItem == null ){
			logger.debug("Encoder System List empty");
			return result;
		}
		
		if( hasUhd && systemItem.uhd_use != 1 ){
			return result;
		}
										
		if( systemItem.assign + assign <= maxAssign ){							
			result = true;								 
		}		
		
		return result;
	}
	
	protected static Boolean getChkSystemAssign( SystemItem systemItem , int assign, boolean hasUhd, HashMap< Long, Integer > mapTempEncAssign )
	{
		Boolean result = false;
		
		int maxAssign = 1300;
		try {
			String strMaxAssign = new DbCommonHelper().GetConfigureDBValue(null, "enc_max_assign");
			if( strMaxAssign != null && common.Util.isNumber( strMaxAssign ) ) {
				maxAssign = Integer.valueOf( strMaxAssign );
			}	
		}catch( Exception ex ) {}			
		
		Integer nAlreayAssign = mapTempEncAssign.get( systemItem.id );
		if( nAlreayAssign == null ) {
			nAlreayAssign = 0;
		}

		if( systemItem == null ){
			logger.debug("Encoder System List empty");
			return result;
		}
		
		if( hasUhd && systemItem.uhd_use != 1 ){
			return result;
		}
										
		if( ( systemItem.assign + nAlreayAssign.intValue() ) + assign <= maxAssign ){							
			result = true;								 
		}		
		
		return result;
	}

	protected static Boolean getChkSystemAssign(SystemItem systemItem, boolean hasUhd, HashMap<Long, Integer> mapTempEncAssign) {
		Boolean result = false;
		int nAssign = systemItem.nAssign;
		int maxAssign = 1300;

		try {
			String strMaxAssign = String.valueOf(systemItem.encMaxAssign);
			if (strMaxAssign != null && common.Util.isNumber(strMaxAssign)) {
				maxAssign = Integer.valueOf(strMaxAssign);
			} else {
				String configureDBValue = new DbCommonHelper().GetConfigureDBValue(null, "enc_max_assign");
				if (configureDBValue != null && common.Util.isNumber(configureDBValue)) {
					maxAssign = Integer.valueOf(configureDBValue);
				}
			}

			Integer nAlreayAssign = mapTempEncAssign.get(systemItem.id);
			if (nAlreayAssign == null) {
				nAlreayAssign = 0;
			}

			if (systemItem == null) {
				logger.debug("Encoder System List empty");
				return result;
			}

			if (hasUhd && systemItem.uhd_use != 1) {
				return result;
			}

			if ((systemItem.assign + nAlreayAssign.intValue()) + nAssign <= maxAssign) {
				result = true;
			}

//			mapTempEncAssign.put("nAssign", assign);

		} catch (Exception e) {
			logger.error("", e);
		}

		return result;
	}
		
	protected static ArrayList<SystemItem> getSystemInfo( String strEncVendor, boolean bUseTV, boolean bUseMobile, boolean bUseClip, boolean bRetry )
	{
		ArrayList<SystemItem> resultList = new ArrayList<>();
		
		try {
			do {				
				String strAddSql = GetSystemInfoAddSql( bUseTV, bUseMobile, bUseClip, bRetry );
				if( strAddSql == null || strAddSql.isEmpty() ) {
					logger.error("GetSystemInfoAddSql Func Get Add Sql Text Failure");
					break;
				}
			
				DbTransaction db = new DbTransaction();
				Connection conn = null;
				try {
					conn = db.startAutoCommit();
					String sql = null;
					
					// type = Encoder
					// status = 1 (동작)
					// auto = 1 (자동)							
					sql = String.format("SELECT * FROM cems.system WHERE cems.system.status=1 AND cems.system.auto=1 AND cems.system.type='Encoder' AND vendor='%s' %s", strEncVendor, strAddSql );
					try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
						logger.info( pstmt.toString() );
						try( ResultSet rs = pstmt.executeQuery() ){
							while (rs.next()) {
								SystemItem item = new SystemItem();
								item.id = rs.getLong("id");
								item.name = rs.getString("name");
								item.assign = rs.getInt("assign");
								item.usageType = rs.getString("div_usage");
								item.vendor = rs.getString("vendor");
								item.strAuthorizeID = rs.getString("authorize_id");
								item.strAuthorizePW = rs.getString("authorize_pw");
								item.strPortApi = rs.getString("port_api");
								int uhdVal = rs.getInt("uhd_use");
								if( !rs.wasNull() ){
									item.uhd_use = uhdVal;
								}else{
									item.uhd_use = 0;
								}
								
								resultList.add(item);
							}
						}
					}												
					
				} catch (Exception e) {
					logger.error("", e);
				} finally {
					db.close();
				}
				
			}while( false );
			
		}catch( Exception ex ) {
			logger.error("",ex);
		}
		
		return resultList;
	}

	protected static ArrayList<SystemItem> getSystemInfo(ArrayList<Define.IDItem> listCids, String strEncVendor, boolean bUseTV, boolean bUseMobile, boolean bUseClip, boolean bRetry )
	{
		ArrayList<SystemItem> resultList = new ArrayList<>();

		try {
			do {
				String strAddSql = GetSystemInfoAddSql( bUseTV, bUseMobile, bUseClip, bRetry );
				if( strAddSql == null || strAddSql.isEmpty() ) {
					logger.error("GetSystemInfoAddSql Func Get Add Sql Text Failure");
					break;
				}

				DbTransaction db = new DbTransaction();
				Connection conn = null;
				try {
					conn = db.startAutoCommit();
					String sql = null;

					// type = Encoder
					// status = 1 (동작)
					// auto = 1 (자동)
					sql = String.format("SELECT * FROM cems.system WHERE cems.system.status=1 AND cems.system.auto=1 AND cems.system.type='Encoder' AND vendor='%s' %s", strEncVendor, strAddSql );
					try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
						logger.info( pstmt.toString() );
						try( ResultSet rs = pstmt.executeQuery() ){
							while (rs.next()) {
								SystemItem item = new SystemItem();
								item.id = rs.getLong("id");
								item.name = rs.getString("name");
								item.assign = rs.getInt("assign");
								item.usageType = rs.getString("div_usage");
								item.vendor = rs.getString("vendor");
								item.strAuthorizeID = rs.getString("authorize_id");
								item.strAuthorizePW = rs.getString("authorize_pw");
								item.strPortApi = rs.getString("port_api");
								int uhdVal = rs.getInt("uhd_use");
								if( !rs.wasNull() ){
									item.uhd_use = uhdVal;
								}else{
									item.uhd_use = 0;
								}

								item.encMaxAssign = rs.getInt("enc_max_assign");

								int nAssign = 0;
								int nAssignTemp = 0;

								for (Define.IDItem idItem : listCids) {
									String selectContentSql = null;
									if (idItem.eIDType == Define.IDType.RTSP) {
										selectContentSql = "SELECT * FROM cems.content where cid = '" + idItem.strID + "'";
									} else if (idItem.eIDType == Define.IDType.HLS) {
										selectContentSql = "SELECT * FROM content_hls_enc WHERE cid  = '" + idItem.strID + "'";
									}
									logger.info(String.format(" select content sql [%s]", selectContentSql));



									try (PreparedStatement pstmt2 = conn.prepareStatement(selectContentSql)) {
										try (ResultSet rs2 = pstmt2.executeQuery()) {
											while (rs2.next()) {
												int pid = rs2.getInt("profile_id");
												String strEqualRtspCid = rs2.getString("equal_rtsp_cid");
												logger.info(String.format(" pid | [%s]", pid));

												ProfileAssignInfo.ProfileInfo proInfo = ProfileAssignInfo.getProfileType(conn, pid);
												String type = proInfo.type;
												String resolution = proInfo.resolution;

												String targetColumn = getColumn(type, resolution);
												nAssignTemp = rs.getInt(targetColumn);

												if (idItem.eIDType == Define.IDType.HLS && strEqualRtspCid != null && !strEqualRtspCid.isEmpty()) {
													nAssignTemp = 0;
												}
												nAssign += nAssignTemp;
											}
										}
									}
								}
								item.assign = nAssign;
								resultList.add(item);
							}
						}
					}

				} catch (Exception e) {
					logger.error("", e);
				} finally {
					db.close();
				}

			}while( false );

		}catch( Exception ex ) {
			logger.error("",ex);
		}

		return resultList;
	}
	
	private static String GetSystemInfoAddSql( boolean bUseTV, boolean bUseMobile, boolean bUseClip, boolean bRetry ) throws Exception
	{
		String strRet = null;
		
		do {
			String strAddSql = "";
			
			if( bUseTV ) {
				strAddSql += " div_usage like '%%TV%%' ";
			}
			
			if( bUseMobile ) {
				
				if( !strAddSql.isEmpty() ) {
					strAddSql += " OR ";
				}
				
				strAddSql += " div_usage like '%%Mobile%%' ";
			}			
			
			if( bUseClip ) {
				
				if( !strAddSql.isEmpty() ) {
					strAddSql += " OR ";
				}
				
				strAddSql += " div_usage like '%%Clip%%' ";
			}
			
			strRet = String.format(" AND ( %s ) ", strAddSql );						
			strRet += String.format(" AND type_retry in ( %s ) ", bRetry ? " 1, 2 " : " 0, 2 " );
			
		}while( false );
		
		return strRet;
	}
	
	protected static ArrayList<SystemItem> getSystemInfoClip( String strEncVendor )
	{
		ArrayList<SystemItem> resultList = new ArrayList<>();
		
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		try {
			conn = db.startAutoCommit();
			String sql = null;
			
			// type = Encoder
			// status = 1 (동작)
			// auto = 1 (자동)			
			sql = "SELECT * FROM cems.system WHERE cems.system.status=1 AND cems.system.auto=1 AND cems.system.type='Encoder' AND vendor=? AND div_usage like '%%Clip%%'";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {				
				pstmt.setString(1, strEncVendor );				
				try( ResultSet rs = pstmt.executeQuery() ){
					while (rs.next()) {
						SystemItem item = new SystemItem();
						item.id = rs.getLong("id");
						item.name = rs.getString("name");
						item.assign = rs.getInt("assign");
						item.usageType = rs.getString("div_usage");
						item.vendor = rs.getString("vendor");
						item.strAuthorizeID = rs.getString("authorize_id");
						item.strAuthorizePW = rs.getString("authorize_pw");
						int uhdVal = rs.getInt("uhd_use");
						if( !rs.wasNull() ){
							item.uhd_use = uhdVal;
						}else{
							item.uhd_use = 0;
						}
						
						resultList.add(item);
					}
				}
			}												
			
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			db.close();
		}
		
		return resultList;
	}
	
	
	private static SystemItem getSystemInfo( long sid, boolean bUseTV, boolean bUseMobile, boolean bUseClip, boolean bRetry )
	{			
		
		try {
			
			do {
				String strAddSql = GetSystemInfoAddSql( bUseTV, bUseMobile, bUseClip, bRetry );
				if( strAddSql == null || strAddSql.isEmpty() ) {
					logger.error("GetSystemInfoAddSql Func Get Add Sql Text Failure");
					break;
				}				
			
				DbTransaction db = new DbTransaction();
				Connection conn = null;
				try {
					conn = db.startAutoCommit();
					String sql = null;
					
					// type = Encoder
					// status = 1 (동작)
					// auto = 1 (자동)
					sql = String.format( "SELECT * FROM cems.system WHERE cems.system.id=? AND cems.system.status=1 AND cems.system.type='Encoder' AND vendor='ELEMENTAL' %s", strAddSql );
					try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
						pstmt.setLong(1, sid);
						logger.info( pstmt.toString() );
						try( ResultSet rs = pstmt.executeQuery() ){
							if (rs.next()) {
								SystemItem item = new SystemItem();
								item.id = rs.getLong("id");
								item.assign = rs.getInt("assign");
								item.strAuthorizeID = rs.getString("authorize_id");
								item.strAuthorizePW = rs.getString("authorize_pw");
								item.strPortApi = rs.getString("port_api");
								int uhdVal = rs.getInt("uhd_use");
								if( !rs.wasNull() ){
									item.uhd_use = uhdVal;
								}else{
									item.uhd_use = 0;
								}
								
								return item;
							}
						}
					}												
					
				} catch (Exception e) {
					logger.error("", e);
				} finally {
					db.close();
				}
								
			}while( false );
			
		}catch( Exception ex ) {
			logger.error("",ex);
		}
		
		return null;
	}
	
	protected static SystemItem getSystemInfoEx( String strEncVendor, long sid, boolean bUseTV, boolean bUseMobile, boolean bUseClip )
	{			
		
		try {
			
			do {							
			
				DbTransaction db = new DbTransaction();
				Connection conn = null;
				try {
					conn = db.startAutoCommit();
					String sql = null;
					
					// type = Encoder
					// status = 1 (동작)
					// auto = 1 (자동)
					sql = String.format( "SELECT * FROM cems.system WHERE cems.system.id=? AND cems.system.type='Encoder' AND vendor=?" );
					try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
						pstmt.setLong(1, sid);
						pstmt.setString(2, strEncVendor);						
						logger.info( pstmt.toString() );
						try( ResultSet rs = pstmt.executeQuery() ){
							if (rs.next()) {
								SystemItem item = new SystemItem();
								item.id = rs.getLong("id");
								item.assign = rs.getInt("assign");
								item.strAuthorizeID = rs.getString("authorize_id");
								item.strAuthorizePW = rs.getString("authorize_pw");
								item.strPortApi = rs.getString("port_api");
								int uhdVal = rs.getInt("uhd_use");
								if( !rs.wasNull() ){
									item.uhd_use = uhdVal;
								}else{
									item.uhd_use = 0;
								}
								
								return item;
							}
						}
					}												
					
				} catch (Exception e) {
					logger.error("", e);
				} finally {
					db.close();
				}
								
			}while( false );
			
		}catch( Exception ex ) {
			logger.error("",ex);
		}
		
		return null;
	}

	protected static SystemItem getSystemInfoEx(String strEncVendor, ArrayList<Define.IDItem> listCids, long sid, boolean bUseTV, boolean bUseMobile, boolean bUseClip )
	{

		try {

			do {

				DbTransaction db = new DbTransaction();
				Connection conn = null;
				try {
					conn = db.startAutoCommit();
					String sql = null;
					// type = Encoder
					// status = 1 (동작)
					// auto = 1 (자동)
					sql = String.format( "SELECT * FROM cems.system WHERE cems.system.id=? AND cems.system.type='Encoder' AND vendor=?" );
					try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
						pstmt.setLong(1, sid);
						pstmt.setString(2, strEncVendor);
						logger.info( pstmt.toString() );
						int nAssign = 0;
						int nAssignTemp = 0;

						try( ResultSet rs = pstmt.executeQuery() ){
							if (rs.next()) {
								SystemItem item = new SystemItem();
								item.id = rs.getLong("id");
								item.assign = rs.getInt("assign");
								item.strAuthorizeID = rs.getString("authorize_id");
								item.strAuthorizePW = rs.getString("authorize_pw");
								item.strPortApi = rs.getString("port_api");
								int uhdVal = rs.getInt("uhd_use");
								if( !rs.wasNull() ){
									item.uhd_use = uhdVal;
								}else{
									item.uhd_use = 0;
								}

								item.encMaxAssign = rs.getInt("enc_max_assign");



								for (Define.IDItem idItem : listCids) {
									String selectContentSql = null;
									if (idItem.eIDType == Define.IDType.RTSP) {
										selectContentSql = "SELECT * FROM cems.content where cid = '" + idItem.strID + "'";
									} else if (idItem.eIDType == Define.IDType.HLS) {
										selectContentSql = "SELECT * FROM content_hls_enc WHERE cid  = '" + idItem.strID + "'";
									}
									logger.info(String.format(" select content sql [%s]", selectContentSql));

									try (PreparedStatement pstmt2 = conn.prepareStatement(selectContentSql)) {
										try (ResultSet rs2 = pstmt2.executeQuery()) {
											while (rs2.next()) {
												int pid = rs2.getInt("profile_id");
												String strEqualRtspCid = rs2.getString("equal_rtsp_cid");
												logger.info(String.format(" pid | [%s]", pid));

												ProfileAssignInfo.ProfileInfo proInfo = ProfileAssignInfo.getProfileType(conn, pid);
												String type = proInfo.type;
												String resolution = proInfo.resolution;

												String targetColumn = getColumn(type, resolution);
												nAssignTemp = rs.getInt(targetColumn);

												if( idItem.eIDType == Define.IDType.HLS && strEqualRtspCid != null && !strEqualRtspCid.isEmpty() ) {
													nAssignTemp = 0;
												}

												nAssign += nAssignTemp;
											}
										}
									}
								}
								item.nAssign = nAssign;
								return item;
							}
						}
					}

				} catch (Exception e) {
					logger.error("", e);
				} finally {
					db.close();
				}

			}while( false );

		}catch( Exception ex ) {
			logger.error("",ex);
		}

		return null;
	}
	
	protected static SystemItem getSystemInfoClip( String strEncVendor, long sid )
	{			
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		try {
			conn = db.startAutoCommit();
			String sql = null;
			
			// type = Encoder
			// status = 1 (동작)
			// auto = 1 (자동)			
			//sql = "SELECT * FROM cems.system WHERE cems.system.id=? AND cems.system.status=1 AND cems.system.auto=1 AND cems.system.type='Encoder' AND vendor='ELEMENTAL'";
			sql = "SELECT * FROM cems.system WHERE cems.system.id=? AND cems.system.status=1 AND cems.system.type='Encoder' AND vendor=? AND div_usage like '%%Clip%%'";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setLong(1, sid);
				pstmt.setString(2, strEncVendor);				
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {
						SystemItem item = new SystemItem();
						item.id = rs.getLong("id");
						item.assign = rs.getInt("assign");
						item.strAuthorizeID = rs.getString("authorize_id");
						item.strAuthorizePW = rs.getString("authorize_pw");
						item.strPortApi = rs.getString("port_api");
						int uhdVal = rs.getInt("uhd_use");
						if( !rs.wasNull() ){
							item.uhd_use = uhdVal;
						}else{
							item.uhd_use = 0;
						}
						
						return item;
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

	public static String getColumn(String proType, String proResolution){
		logger.info(String.format("get Column params values = proType [%s] | proResolution [%s]", proType, proResolution));
		String sbColumn = null;

		if(proType.toLowerCase().equals("tv")){
			sbColumn = "tv_"+proResolution.toLowerCase()+"_assign";
		}else if(proType.toLowerCase().equals("mobile") || proType.toLowerCase().equals("clip")){
			sbColumn = "mobile_"+proResolution.toLowerCase()+"_assign";
		}

		logger.info(String.format("get Column params values = sbColumn [%s] ", sbColumn));

		return sbColumn;
	}
}
