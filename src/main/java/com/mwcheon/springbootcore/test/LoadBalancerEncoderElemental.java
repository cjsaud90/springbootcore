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

public class LoadBalancerEncoderElemental extends LoadBalancerEncoderBase {
	private static Logger logger = LoggerFactory.getLogger(LoadBalancerEncoderElemental.class);
		
	public static int GetElementalEncCnt() throws Exception
	{
		int nRet = 0;
		
		try{
			DbTransaction db = new DbTransaction();
			Connection conn = null;
			
			conn = db.startAutoCommit();
			try{
				String sql = "select count(*) from cems.system where cems.system.type='Encoder' AND vendor='ELEMENTAL'";
				try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
					ResultSet rs = pstmt.executeQuery();
					if( rs.next() ) {
						nRet = rs.getInt(1);
					}					
				}
								
			}catch(Exception e){
				logger.error("",e);
				throw e;
			} finally {
				db.close();
			}
			
		}catch(Exception e){
			logger.error("",e);
			throw e;
		}
		
		return nRet;
	}		
	
	public synchronized static Long available( boolean hasUhd, int assign, Boolean nwIOChk, boolean bUseTV, boolean bUseMobile, boolean bUseClip, boolean bRetry, HashMap<Long, Integer> mapTempEncAssign ) throws Exception 
	{	
		long result = -1;
				
		ArrayList<SystemItem> systemList = getSystemInfo( EncVendor.ELEMENTAL.toString(), bUseTV, bUseMobile, bUseClip, bRetry );
		if( systemList == null || systemList.isEmpty() ){
			logger.info("Encoder System is Empty");
			return result;
		}
		
		result = getChkSystemAssignEx(systemList, assign, hasUhd, mapTempEncAssign );
		
		//[원본]다운로드 체크
		if( nwIOChk ){
			if( !nwIOCheck( result ) ){
				result = -1;
			}					
		}
		
		return result;
	}

	public synchronized static Map<String, Object>  available( ArrayList<Define.IDItem> listCids, boolean hasUhd, Boolean nwIOChk, boolean bUseTV, boolean bUseMobile, boolean bUseClip, boolean bRetry, HashMap<Long, Integer> mapTempEncAssign ) throws Exception
	{
		Map<String, Object> result = new HashMap<>();
		result.put("sid", -1);

		ArrayList<SystemItem> systemList = getSystemInfo(listCids, EncVendor.ELEMENTAL.toString(), bUseTV, bUseMobile, bUseClip, bRetry );
		if( systemList == null || systemList.isEmpty() ){
			logger.info("Encoder System is Empty");
			return result;
		}
		Map<String, Object> chkSystemAssignEx = getChkSystemAssignEx(systemList, hasUhd, mapTempEncAssign);

		result.put("sid", chkSystemAssignEx.get("sid"));
		result.put("nAssign", chkSystemAssignEx.get("nAssign"));

		//[원본]다운로드 체크
		if( nwIOChk ){
			if( !nwIOCheck((Long) result.get("result")) ){
				result.put("sid", -1);

			}
		}

		return result;
	}
	
	public synchronized static Long available( boolean hasUhd, int assign, Boolean nwIOChk, boolean bUseTV, boolean bUseMobile, boolean bUseClip, boolean bRetry ) throws Exception 
	{	
		long result = -1;
				
		ArrayList<SystemItem> systemList = getSystemInfo( EncVendor.ELEMENTAL.toString(), bUseTV, bUseMobile, bUseClip, bRetry );
		if( systemList == null || systemList.isEmpty() ){
			logger.info("Encoder System is Empty");
			return result;
		}
		
		
		result = getChkSystemAssign(systemList, assign, hasUhd );
		//[원본]다운로드 체크
		if( nwIOChk ){
			if( !nwIOCheck( result ) ){
				result = -1;
			}					
		}
		
		return result;
	}
	
	public synchronized static Long availableClip( boolean hasUhd, int assign, Boolean nwIOChk ) throws Exception 
	{	
		long result = -1;
		
		ArrayList<SystemItem> systemList = getSystemInfoClip( EncVendor.ELEMENTAL.toString() );
		if( systemList == null || systemList.isEmpty() ){
			//logger.debug("Encoder System is Empty");
			return result;
		}
		
		result = getChkSystemAssign(systemList, assign, hasUhd );
		
		//[원본]다운로드 체크
		if( nwIOChk ){
			if( !nwIOCheck( result ) ){
				result = -1;
			}					
		}
		
		return result;
	}
	
	public synchronized static boolean available(long sid, boolean hasUhd, int assign, Boolean nwIOChk, boolean bUseTV, boolean bUseMobile, boolean bUseClip ) throws Exception {
		boolean result = false;
				
		SystemItem sysItem = getSystemInfoEx( EncVendor.ELEMENTAL.toString(), sid, bUseTV, bUseMobile, bUseClip );
		if( sysItem == null ){
			logger.debug("Encoder System is Empty");
			return result;
		}
		
		result = getChkSystemAssign(sysItem, assign, hasUhd );
		
		//[원본]다운로드 체크
		if( nwIOChk ){
			if( !nwIOCheck( sid ) ){
				result = false;
			}					
		}
		
		return result;
	}
	
	public synchronized static boolean available(long sid, boolean hasUhd, int assign, Boolean nwIOChk, boolean bUseTV, boolean bUseMobile, boolean bUseClip, HashMap< Long, Integer> mapTempEncAssign ) throws Exception {
		boolean result = false;

		SystemItem sysItem = getSystemInfoEx( EncVendor.ELEMENTAL.toString(), sid, bUseTV, bUseMobile, bUseClip );
		if( sysItem == null ){
			logger.debug("Encoder System is Empty");
			return result;
		}

		result = getChkSystemAssign( sysItem, assign, hasUhd, mapTempEncAssign );

		//[원본]다운로드 체크
		if( nwIOChk ){
			if( !nwIOCheck( sid ) ){
				result = false;
			}
		}

		return result;
	}

	public synchronized static Map<String, Object> available(long sid, ArrayList<Define.IDItem> listCids, boolean hasUhd, Boolean nwIOChk, boolean bUseTV, boolean bUseMobile, boolean bUseClip, HashMap< Long, Integer> mapTempEncAssign ) throws Exception {
		Map<String, Object> result = new HashMap<>();
		result.put("result", false);

		SystemItem sysItem = getSystemInfoEx( EncVendor.ELEMENTAL.toString(), listCids, sid, bUseTV, bUseMobile, bUseClip );
		if( sysItem == null ){
			logger.debug("Encoder System is Empty");
			return result;
		}

		result.put("result", getChkSystemAssign( sysItem,  hasUhd, mapTempEncAssign ));
		result.put("nAssign", sysItem.nAssign);

		//[원본]다운로드 체크
		if( nwIOChk ){
			if( !nwIOCheck( sid ) ){
				result.put("result", false);
			}
		}

		return result;
	}
	
	public synchronized static boolean availableClip(long sid, boolean hasUhd, int assign, Boolean nwIOChk ) throws Exception {
		boolean result = false;
		
		SystemItem sysItem = getSystemInfoClip( EncVendor.ELEMENTAL.toString(), sid );
		if( sysItem == null ){
			//logger.debug("Encoder System is Empty");
			return result;
		}
		
		result = getChkSystemAssign(sysItem, assign, hasUhd );
		
		//[원본]다운로드 체크
		if( nwIOChk ){
			if( !nwIOCheck( sid ) ){
				result = false;
			}					
		}
		
		return result;
	}
}
