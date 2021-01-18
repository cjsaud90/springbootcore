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

import common.system.LoadBalancerEncoderBase.EncVendor;

public class LoadBalancerEncoderSuperNova extends LoadBalancerEncoderBase {
	private static Logger logger = LoggerFactory.getLogger(LoadBalancerEncoderSuperNova.class);

	public synchronized static Long available( int assign, boolean hasUhd, boolean bUseTV, boolean bUseMobile, boolean bUseClip, HashMap< Long, Integer > mapTempEncAssign  ) throws Exception 
	{	
		long result = -1;
				
		ArrayList<SystemItem> systemList = getSystemInfo( EncVendor.SUPERNOVA.toString(), bUseTV, bUseMobile, bUseClip, false );
		if( systemList == null || systemList.isEmpty() ){
			//logger.info("Encoder System is Empty");
			return result;
		}
		
		result = getChkSystemAssignEx( systemList, assign, hasUhd, mapTempEncAssign );
		
		return result;
	}

	public synchronized static Map<String, Object> available( ArrayList<Define.IDItem> listCids, boolean hasUhd, boolean bUseTV, boolean bUseMobile, boolean bUseClip, HashMap< Long, Integer > mapTempEncAssign  ) throws Exception
	{
		Map<String, Object> result = new HashMap<>();
		result.put("sid", -1);

		ArrayList<SystemItem> systemList = getSystemInfo( listCids, EncVendor.SUPERNOVA.toString(), bUseTV, bUseMobile, bUseClip, false );
		if( systemList == null || systemList.isEmpty() ){
			//logger.info("Encoder System is Empty");
			return result;
		}

		Map<String, Object> chkSystemAssignEx = getChkSystemAssignEx(systemList, hasUhd, mapTempEncAssign);

		result.put("sid", chkSystemAssignEx.get("sid"));
		result.put("nAssign", chkSystemAssignEx.get("nAssign"));

		return result;
	}
	
	public synchronized static Long availableClip( boolean hasUhd, int assign ) throws Exception 
	{	
		long result = -1;
		
		ArrayList<SystemItem> systemList = getSystemInfoClip( EncVendor.SUPERNOVA.toString() );
		if( systemList == null || systemList.isEmpty() ){
			//logger.debug("Encoder System is Empty");
			return result;
		}
		
		result = getChkSystemAssign(systemList, assign, hasUhd );
				
		return result;
	}
	
	public synchronized static boolean available(long sid, boolean hasUhd, int assign ) throws Exception {
		boolean result = false;
				
		SystemItem sysItem = getSystemInfoEx( EncVendor.SUPERNOVA.toString(), sid, true, true, true );
		if( sysItem == null ){
			//logger.debug("Encoder System is Empty");
			return result;
		}
		
		result = getChkSystemAssign(sysItem, assign, hasUhd );
			
		return result;
	}

	public synchronized static Map<String, Object> available(ArrayList<Define.IDItem> listCids, long sid, boolean hasUhd ) throws Exception {
		Map<String, Object> result = new HashMap<>();
		result.put("result", false);

		SystemItem sysItem = getSystemInfoEx( EncVendor.SUPERNOVA.toString(), listCids, sid, true, true, true );
		if( sysItem == null ){
			//logger.debug("Encoder System is Empty");
			return result;
		}

		result.put("result", getChkSystemAssign(sysItem, 0, hasUhd ));
		result.put("nAssign", sysItem.nAssign );
		return result;
	}
	
	public synchronized static boolean availableClip(long sid, boolean hasUhd, int assign ) throws Exception {
		boolean result = false;
		
		SystemItem sysItem = getSystemInfoClip( EncVendor.SUPERNOVA.toString(), sid );
		if( sysItem == null ){
			//logger.debug("Encoder System is Empty");
			return result;
		}
		
		result = getChkSystemAssign(sysItem, assign, hasUhd );
		
		return result;
	}
	
}
