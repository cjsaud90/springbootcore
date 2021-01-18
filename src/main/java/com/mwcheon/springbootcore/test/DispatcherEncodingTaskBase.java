package com.mwcheon.springbootcore.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import common.DbCommonHelper;
import common.DbTransaction;
import common.Define;
import common.ExtraStatusManipulation;
import common.ProfileAssignInfo;
import common.Util;
import controller.TranscodingStatus;
import controller.TranscodingTask;
import service.automation.DispatcherEncodingTaskBase.EncAssignItemEx;
import service.automation.DispatcherEncodingTaskBase.ReadyItem;
import task.pipeline.MobileEncodingPipeline;
import task.pipeline.Pipeline;
import task.pipeline.TvEncodingPipeline;
import task.pipeline.TvMobileEncodingPipeline;

public class DispatcherEncodingTaskBase extends Thread {
	private static Logger logger = LoggerFactory.getLogger(DispatcherEncodingTaskElemental.class);	
	
	protected final int nScheTypeTV = 1 << 1;
	protected final int nScheTypeMobile = 1 << 2;
	
	protected enum enc_type
	{
		None,
		Elemenatal,
		Galaxia,
	}

	protected class ReadyItem {
		String strMediaID;
		String strEpsdID;
		String cid;
		String type;
		Long sid;
		String frameSize;
		enc_type encType;
		boolean hasUhd;
		Define.IDType eIDType;
		
		Long lProfileIdSuperNova;		
		
		public ReadyItem()
		{
			encType = enc_type.None;
			lProfileIdSuperNova = null;
		}
	}
	
	protected class EncAssignItemEx
	{
		public ArrayList<Define.IDItem> listCids;
		public int nAssign;
		public boolean bIsUHD;
		public boolean bRetry;
		public Pipeline pipeline;
		public Long lSid; 				
		public int nScheType;

		public EncAssignItemEx( ArrayList<Define.IDItem> listCids, boolean bIsUHD, int nScheType, long lSid, boolean bRetry )
		{
			this.listCids = listCids;
			this.nAssign = 0;
			this.bIsUHD = bIsUHD;
			this.pipeline = null;
			this.lSid = null;
			this.nScheType = nScheType;
			this.lSid = lSid;
			this.bRetry = bRetry;
		}

		public EncAssignItemEx( ArrayList<Define.IDItem> listCids, int nAssign, boolean bIsUHD, int nScheType, long lSid, boolean bRetry )
		{
			this.listCids = listCids;
			this.nAssign = nAssign;
			this.bIsUHD = bIsUHD;
			this.pipeline = null;
			this.lSid = null;						
			this.nScheType = nScheType;
			this.lSid = lSid;
			this.bRetry = bRetry;
		}		
	}
		
	protected boolean GetIncludeSysDivUsage( int ChkVal, int nSysDivUsageCode ) throws Exception
	{
		boolean bRet = false;
		
		try {
			if( ( ChkVal & nSysDivUsageCode ) != 0 ) {
				bRet = true;
			}			
		}catch( Exception ex ) {
			logger.error("",ex);			
		}
		
		return bRet;
	}
		
	protected ArrayList<Util.contIDInfo> getProfileID(String tbl, String cid) {
		ArrayList<Util.contIDInfo> contIDList = new ArrayList<>();
		
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		
		int count = 0;

		try {
			conn = db.startAutoCommit();			
			String sql = null;			
			sql = "SELECT * FROM " + tbl + " WHERE cid=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setString(1, cid);
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {				
						String setNum = rs.getString("set_num");
						if (setNum != null && setNum.length() > 0) {
							sql = "SELECT * FROM " + tbl + " WHERE set_num=?";
							try (PreparedStatement pstmt2 = conn.prepareStatement(sql)) {
								pstmt2.setString(1, setNum);
								try( ResultSet rs2 = pstmt2.executeQuery() ){					
									while (rs2.next()) {
										Integer pid = (Integer)rs2.getObject("profile_id");
										if( pid != null ){
											Util.contIDInfo infoItem = new Util.contIDInfo();
											infoItem.cid = rs2.getString("cid");
											infoItem.pid = pid;
											infoItem.tbl = tbl;
											infoItem.type = rs2.getString("type");
											contIDList.add(infoItem);
										}
									}
								}
							}
						}else {
							Integer pid = (Integer)rs.getObject("profile_id");
							if( pid != null ){
								Util.contIDInfo infoItem = new Util.contIDInfo();
								infoItem.cid = cid;
								infoItem.pid = pid;
								infoItem.tbl = tbl;
								infoItem.type = rs.getString("type");
								contIDList.add(infoItem);
							}						
						}
					}
				}
			}

			return contIDList;
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			db.close();
		}
		return null;
	}
	
	protected Pipeline createPipeline( int nUseType ) throws Exception {
		
		boolean tvType = ( nUseType & nScheTypeTV ) != 0 ? true : false;
		boolean moType = ( nUseType & nScheTypeMobile ) != 0 ? true : false;

		if (tvType && moType) {
			return new TvMobileEncodingPipeline(); 
		} else if (tvType) {
			return new TvEncodingPipeline();
		} else if (moType) {
			return new MobileEncodingPipeline();
		}
		
		return null;
	}
	
	protected void setWorkingStatusEx( ArrayList<EncAssignItemEx> listEncAssignItem ) throws Exception {
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		
		try {
			conn = db.start();
			
			try (PreparedStatement pstmt = conn.prepareStatement("UPDATE cems.content SET status='[원본]작업준비', sid=? WHERE cid=?")) {
				for (EncAssignItemEx encAssignItem : listEncAssignItem ) {
					for( Define.IDItem idItem : encAssignItem.listCids ) {
						if( idItem.eIDType !=  Define.IDType.RTSP ) {
							continue;
						}
						
						int args = 1;
						pstmt.setLong(args++, encAssignItem.lSid.longValue());
						pstmt.setString(args++, idItem.strID);
						logger.info(pstmt.toString());
						pstmt.executeUpdate();
					}					
				}
			}
			
			try (PreparedStatement pstmt = conn.prepareStatement("UPDATE cems.content_hls_enc SET status='[원본]작업준비', sid=? WHERE cid=?")) {
				for (EncAssignItemEx encAssignItem : listEncAssignItem ) {
					for( Define.IDItem idItem : encAssignItem.listCids ) {
						if( idItem.eIDType !=  Define.IDType.HLS ) {
							continue;
						}
						
						int args = 1;
						pstmt.setLong(args++, encAssignItem.lSid.longValue());
						pstmt.setString(args++, idItem.strID);
						logger.info(pstmt.toString());
						pstmt.executeUpdate();								
					}										
				}
			}
			
			db.commit();
			
			//TranscodingTask.signal();
			//TranscodingStatus.signal();						
			//RawTaskSchedule.signal();
			//BroadcastTaskManager.signal();
			//TsTaskSchedule.signal();			
			
		} catch (Exception e) {
			db.rollback();
			throw e;
		} finally {
			db.close();
		}
	}
		
	protected void setWorkingStatus(String tbl, ArrayList<Util.SystemAssignInfo> sysAssignInfo) throws Exception {
		ArrayList<String> cids = new ArrayList<>();

		DbTransaction db = new DbTransaction();
		Connection conn = null;

		try {
			conn = db.start();
						
			try (PreparedStatement pstmt = conn.prepareStatement("UPDATE " + tbl + " SET status='[원본]작업준비', sid=? WHERE cid=?")) {
				for (Util.SystemAssignInfo item : sysAssignInfo) {
					pstmt.setLong(1, item.sid);
					pstmt.setString(2, item.cid);
					logger.debug(pstmt.toString());
					pstmt.executeUpdate();
				}
			}
			
			db.commit();
			
			TranscodingTask.signal();
			TranscodingStatus.signal();
			
		} catch (Exception e) {
			db.rollback();
			throw e;
		} finally {
			db.close();
		}
	}
	
	// EncPipeline에 할당 할 CIDs 및 Assign 값을 가지고 온다	
	protected ArrayList<EncAssignItemEx> getAssignBySuperNova( ReadyItem item, int nEncMaxAssignVal ) 
	{				
		ArrayList<EncAssignItemEx> listRet = new ArrayList<>();
		ArrayList<EncAssignItemEx> listTemp = new ArrayList<>();
		ArrayList<String> listCheckerID = new ArrayList<>();
		
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		
		ArrayList<Define.IDItem> listCidsNormal = new ArrayList<>();			// RTSP Etc + HLS Etc
		long lSidNoraml = -1;		
		int nAssignNoraml = 0;				
		int nScheType = 0;
		long lExtraStatusMix = ( ExtraStatusManipulation.Flag.EF_NEW_DRM.get() | ExtraStatusManipulation.Flag.EF_WATERMARK.get() | ExtraStatusManipulation.Flag.EF_ADD_ECDN_TASK.get() );
		
		try {
			
			try {
				conn = db.startAutoCommit();			
				String sql = null;			
				
				HashMap< String, String > mapFramesizeTv = new DbCommonHelper().GetMapDbDefineCode(conn, Define.MappingDefineCodeGrpCd.RSLU_CODE_TV.toString() );
				if( mapFramesizeTv == null || mapFramesizeTv.isEmpty() ) {
					throw new Exception( String.format("Db Mapping Define Code Tv Rslu Data is Empty | [ %s ]", Define.MappingDefineCodeGrpCd.RSLU_CODE_TV.toString() ) );
				}
				
				if( item.eIDType == Define.IDType.RTSP ) {
					
					sql = "SELECT * FROM cems.content WHERE cid=? and encd_piqu_typ_cd=?";
					try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
						pstmt.setString(1, item.cid );
						pstmt.setInt(2, 1 );
						try( ResultSet rs = pstmt.executeQuery() ){
							while (rs.next()) {
								String setPossnYN = rs.getString("possn_yn");						
								String setNum = rs.getString("set_num");
								if (setNum != null && setNum.length() > 0) {
									
									//RTSP
									sql = "SELECT * FROM cems.content WHERE encd_piqu_typ_cd=1 AND set_num=? AND ((extra_status & ?) = 0) and hls_yn is null and encd_piqu_typ_cd=?";
									try (PreparedStatement pstmt2 = conn.prepareStatement(sql)) {
										pstmt2.setString(1, setNum);
										pstmt2.setInt(2, ExtraStatusManipulation.Flag.EF_NO_DISPLAY.get() );
										pstmt2.setInt(3, 1 );
										logger.info( pstmt2.toString() );
										try( ResultSet rs2 = pstmt2.executeQuery() ){					
											while (rs2.next()) {											
												int nAssignTemp = 0;
												String strPossnYn = rs2.getString("possn_yn");
												String strcid = rs2.getString("cid");
												String strFramesize = rs2.getString("framesize");
												String strType = rs2.getString("type");
												long lExtraStatus = rs2.getLong("extra_status");
												boolean bIsVituralCid = ( lExtraStatus & lExtraStatusMix ) == 0 ? false : true;
												Integer pid = (Integer)rs2.getObject("profile_id");
												long lSid = rs2.getLong("sid");
												if( (lExtraStatus & ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get()) == 0 || rs2.wasNull() ) {
													lSid = -1;
												}
												
												String strStatus = rs2.getString("status");
												if( strStatus == null || !strStatus.equals("[원본]작업대기") ) {
//													logger.info( String.format("Set Member Status is Not [원본]작업대기 | [ RTSP ] [ %s ] [ CID : %s ] [ Type : %s ] [ Framesize : %s ] [ PossnYn : %s ]", 
//															strStatus,
//															strcid,
//															strType,
//															strFramesize,
//															rs2.getString("possn_yn")
//															) );
													continue;
												}
												
												Define.IDItem idItem = new Define.IDItem(strcid, Define.IDType.RTSP);												
																																				
												if( listCheckerID.contains( strcid) ) {
													continue;
												}
																								
//												try {
//													do {
//														ProfileAssignInfo.ProfileInfo proInfo = ProfileAssignInfo.getProfileType(conn, pid);
//														nAssignTemp = ProfileAssignInfo.getAssign(conn, proInfo.type, proInfo.resolution, proInfo.strDivision );
//													}while( false );
//
//												}catch( Exception ex ) {
//													logger.error("",ex);
//												}
												
												
												logger.info( String.format( "Enc Get Assign SetNum | [ set num : %s ] [ IDType : %s ] [ cid : %s ] [ isVitural : %s ] [ Assign : %d ] [ PossnYn : %s ]", 
														setNum,
														idItem.eIDType.toString(),
														idItem.strID,
														bIsVituralCid ? "True" : "False",
														nAssignTemp,
														strPossnYn
														) );
																															
												if( strType.toUpperCase().equals("TV") && ( strFramesize.toUpperCase().equals("UHD") || strFramesize.toUpperCase().equals("UHD+HDR") ) ) {													
													ArrayList<Define.IDItem> listIdItem = new ArrayList<Define.IDItem>();
													listIdItem.add( idItem );
//													listTemp.add( new EncAssignItemEx( listIdItem, nAssignTemp, true, nScheTypeTV, lSid, false ) );
													listTemp.add( new EncAssignItemEx( listIdItem, true, nScheTypeTV, lSid, false ) );

												}else {
													
													if( nAssignNoraml + nAssignTemp >= nEncMaxAssignVal ) {
														ArrayList<Define.IDItem> listIdItem = new ArrayList<Define.IDItem>();
														listIdItem.add( idItem );
//														listTemp.add( new EncAssignItemEx( 	listIdItem, nAssignTemp, false, strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile, lSid, false) );
														listTemp.add( new EncAssignItemEx( 	listIdItem, false, strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile, lSid, false) );
													}else {
														listCidsNormal.add( idItem );
//														nAssignNoraml += nAssignTemp;
														lSidNoraml = lSid;														
														nScheType = ( nScheType | ( strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile ) );	
													}
													
												}											
												
												listCheckerID.add( strcid );
												
											}
										}
									}
									
									//HLS									
									sql = "SELECT * FROM cems.content_hls_enc WHERE encd_piqu_typ_cd=1 AND set_num=? and encd_piqu_typ_cd=?";
									try (PreparedStatement pstmt2 = conn.prepareStatement(sql)) {
										pstmt2.setString(1, setNum );											
										pstmt2.setInt(2, 1 );
										logger.info( pstmt2.toString() );
										try( ResultSet rs2 = pstmt2.executeQuery() ){
											while( rs2.next() ) {
												
												int nAssignTemp = 0;
												String strEqualRtspCid = rs2.getString("equal_rtsp_cid");
												String strcid = rs2.getString("cid");
												String strFramesize = rs2.getString("framesize");
												String strType = "TV";																														
												Integer pid = (Integer)rs2.getObject("profile_id");
												long lExtraStatus = rs2.getLong("extra_status");
												long lSid = rs2.getLong("sid");
												if( (lExtraStatus & ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get()) == 0 || rs2.wasNull() ) {
													lSid = -1;
												}
												
												Define.IDItem idItem = new Define.IDItem( rs2.getString("mda_id"), strcid, Define.IDType.HLS);												
												
												String strStatus = rs2.getString("status");
												if( strStatus == null || !strStatus.equals("[원본]작업대기") ) {
//													logger.info( String.format("Set Member Status is Not [원본]작업대기 | [ HLS ] [ %s ] [ MediaID : %s ] [ CID : %s ] [ Framesize : %s ]", 
//															strStatus,
//															rs2.getString("mda_id"),
//															strcid,
//															strFramesize															
//															) );
													continue;
												}
												
												if( listCheckerID.contains( strcid ) ) {
													continue;
												}																							
																											
//												try {
//													do {
//														ProfileAssignInfo.ProfileInfo proInfo = ProfileAssignInfo.getProfileType(conn, pid);
//														nAssignTemp = ProfileAssignInfo.getAssign(conn, proInfo.type, proInfo.resolution, proInfo.strDivision );
//													}while( false );
//
//												}catch( Exception ex ) {
//													logger.error("",ex);
//												}
												
//												if( strEqualRtspCid != null && !strEqualRtspCid.isEmpty() ) {
//													nAssignTemp = 0;
//												}
												
												logger.info( String.format( "Enc Get Assign SetNum HLS | [ set num : %s ] [ MDA_RSLU_ID : %s ] [ Assign : %d ] [ strEqualRtspCid : %s ]", 
														setNum,
														strcid,																	
														nAssignTemp,
														strEqualRtspCid
														) );
												
												if( strType.toUpperCase().equals("TV") && ( strFramesize.toUpperCase().equals("UHD") || strFramesize.toUpperCase().equals("UHD+HDR") ) ) {																																						
													ArrayList<Define.IDItem> listIdItem = new ArrayList<Define.IDItem>();
													listIdItem.add( idItem );
//													listTemp.add( new EncAssignItemEx( listIdItem, nAssignTemp, true, nScheTypeTV, lSid, false ) );
													listTemp.add( new EncAssignItemEx( listIdItem, true, nScheTypeTV, lSid, false ) );

												}else {		
													
													if( nAssignNoraml + nAssignTemp >= nEncMaxAssignVal ) {
														ArrayList<Define.IDItem> listIdItem = new ArrayList<Define.IDItem>();
														listIdItem.add( idItem );
//														listTemp.add( new EncAssignItemEx( 	listIdItem, nAssignTemp, false, strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile, lSid, false) );
														listTemp.add( new EncAssignItemEx( 	listIdItem, false, strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile, lSid, false) );
													}else {
													
														listCidsNormal.add( idItem );													
//														nAssignNoraml += nAssignTemp;
														lSidNoraml = lSid;
														nScheType = ( nScheType | nScheTypeTV );
													}
																																		
												}		
												
												listCheckerID.add( strcid );
												
											}																										
										}
									}
																		
								} else if (rs.getString("type").toLowerCase().equals("mobile")) { // mobile only
									
									sql = "SELECT * FROM cems.content WHERE UPPER(type)='MOBILE' AND epsd_id= ? AND ((extra_status & ?) = 0) and status='[원본]작업대기' and possn_yn=? and hls_yn is null and encd_piqu_typ_cd=?";
									try (PreparedStatement pstmt2 = conn.prepareStatement(sql)) {
										pstmt2.setString(1, item.strEpsdID );
										pstmt2.setInt(2, ExtraStatusManipulation.Flag.EF_NO_DISPLAY.get() );
										pstmt2.setString(3, setPossnYN);
										pstmt2.setInt(4, 1 );
										logger.info( pstmt2.toString() );
										try( ResultSet rs2 = pstmt2.executeQuery() ){					
											while (rs2.next()) {										
												
												int nAssignTemp = 0;												
												String strcid = rs2.getString("cid");
												String strFramesize = rs2.getString("framesize");
												String strType = rs2.getString("type");
												long lExtraStatus = rs2.getLong("extra_status");											
												boolean bIsVituralCid = ( lExtraStatus & lExtraStatusMix ) == 0 ? false : true;
												Integer pid = (Integer)rs2.getObject("profile_id");
												long lSid = rs2.getLong("sid");
												if((lExtraStatus & ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get()) == 0 ||  rs2.wasNull() ) {
													lSid = -1;
												}
												
												logger.info( String.format( "Enc Get Assign Mobile | [ CID : %s ]", strcid ) );
												
												String strStatus = rs2.getString("status");
												if( strStatus == null || !strStatus.equals("[원본]작업대기") ) {
//													logger.info( String.format("Set Member Status is Not [원본]작업대기 | [ RTSP ] [ %s ] [ CID : %s ] [ Type : %s ] [ Framesize : %s ] [ PossnYn : %s ]", 
//															strStatus,
//															strcid,
//															strType,
//															strFramesize,
//															rs2.getString("possn_yn")
//															) );
													continue;
												}
												
												Define.IDItem idItem = new Define.IDItem(strcid, Define.IDType.RTSP);												
												
												if( listCheckerID.contains( strcid ) ) {
													continue;
												}
																								
//												try {
//													do {
//														ProfileAssignInfo.ProfileInfo proInfo = ProfileAssignInfo.getProfileType(conn, pid);
//														nAssignTemp = ProfileAssignInfo.getAssign(conn, proInfo.type, proInfo.resolution, proInfo.strDivision);
//
//													}while( false );
//
//												}catch( Exception ex ) {
//													logger.error("",ex);
//												}
												
												logger.info( String.format( "Enc Get Assign Mobile | [ IDType : %s ] [ cid : %s ] [ isVitural : %s ] [ Assign : %d ]",
														idItem.eIDType.toString(),
														idItem.strID,
														bIsVituralCid ? "True" : "False",
														nAssignTemp
														) );
												
												if( nAssignNoraml + nAssignTemp >= nEncMaxAssignVal ) {
													ArrayList<Define.IDItem> listIdItem = new ArrayList<Define.IDItem>();
													listIdItem.add( idItem );
//													listTemp.add( new EncAssignItemEx( 	listIdItem, nAssignTemp, false, strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile, lSid, false) );
													listTemp.add( new EncAssignItemEx( 	listIdItem, false, strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile, lSid, false) );
												}else {
													listCidsNormal.add( idItem );
//													nAssignNoraml += nAssignTemp;
													lSidNoraml = lSid;
													nScheType = ( nScheType | nScheTypeMobile );																																																										
												}
																										
												listCheckerID.add( strcid );
												
											}
										}
									}
									
									
								}else {							
									// RTSP TV Only
									int nAssignTemp = 0;
									String strPossnYn = rs.getString("possn_yn");
									String strcid = rs.getString("cid");
									String strFramesize = rs.getString("framesize");
									String strType = rs.getString("type");
									long lExtraStatus = rs.getLong("extra_status");											
									boolean bIsVituralCid = ( lExtraStatus & lExtraStatusMix ) == 0 ? false : true;
									Integer pid = (Integer)rs.getObject("profile_id");
									long lSid = rs.getLong("sid");
									if( (lExtraStatus & ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get()) == 0 || rs.wasNull() ) {
										lSid = -1;
									}
									
									String strStatus = rs.getString("status");
									if( strStatus == null || !strStatus.equals("[원본]작업대기") ) {
//										logger.info( String.format("Set Member Status is Not [원본]작업대기 | [ RTSP ] [ %s ] [ CID : %s ] [ Type : %s ] [ Framesize : %s ] [ PossnYn : %s ]", 
//												strStatus,
//												strcid,
//												strType,
//												strFramesize,
//												rs.getString("possn_yn")
//												) );
										continue;
									}
									
									Define.IDItem idItem = new Define.IDItem(strcid, Define.IDType.RTSP);																																			
									
									if( listCheckerID.contains( strcid ) ) {
										continue;
									}
									
									logger.info( String.format( "Enc Get Assign Only One | [ IDType : %s ] [ CID : %s ]", idItem.eIDType.toString(), idItem.strID ) );
																		
//									try {
//										do {
//											ProfileAssignInfo.ProfileInfo proInfo = ProfileAssignInfo.getProfileType(conn, pid);
//											nAssignTemp = ProfileAssignInfo.getAssign(conn, proInfo.type, proInfo.resolution, proInfo.strDivision);
//
//										}while( false );
//
//									}catch( Exception ex ) {
//										logger.error("",ex);
//									}
									
									logger.info( String.format( "Enc Get Assign TV | [ IDType : %s ] [ cid : %s ] [ isVitural : %s ] [ Assign : %d ]",
											idItem.eIDType.toString(),
											idItem.strID,
											bIsVituralCid ? "True" : "False",
											nAssignTemp
											) );
																												
									if( strType.toUpperCase().equals("TV") && ( strFramesize.toUpperCase().equals("UHD") || strFramesize.toUpperCase().equals("UHD+HDR") ) ) {																			
										ArrayList<Define.IDItem> listIdItem = new ArrayList<Define.IDItem>();
										listIdItem.add( idItem );
//										listTemp.add( new EncAssignItemEx( listIdItem, nAssignTemp, true, nScheTypeTV, lSid, false ) );
										listTemp.add( new EncAssignItemEx( listIdItem, true, nScheTypeTV, lSid, false ) );

									}else {		
										
										if( nAssignNoraml + nAssignTemp >= nEncMaxAssignVal ) {
											ArrayList<Define.IDItem> listIdItem = new ArrayList<Define.IDItem>();
											listIdItem.add( idItem );
//											listTemp.add( new EncAssignItemEx( 	listIdItem, nAssignTemp, false, strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile, lSid, false) );
											listTemp.add( new EncAssignItemEx( 	listIdItem, false, strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile, lSid, false) );
										}else {
											listCidsNormal.add( idItem );
//											nAssignNoraml += nAssignTemp;
											lSidNoraml = lSid;
											nScheType = ( nScheType | ( strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile ) );
										}
										
									}	
									
									listCheckerID.add( strcid );
									
								}
							}
						}
					}
					
				}else if( item.eIDType == Define.IDType.HLS ) {
					
					ArrayList< String > listHlsSetNum = new ArrayList<>();
					
					sql = "SELECT * FROM cems.content_hls_enc WHERE mda_id=? and encd_piqu_typ_cd=?";
					try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
						pstmt.setString(1, item.strMediaID );
						pstmt.setInt(2, 1 );
						try( ResultSet rs = pstmt.executeQuery() ){
							while( rs.next() ) {
							
								int nAssignTemp = 0;	
								if( rs.getString("set_num") != null && !rs.getString("set_num").isEmpty() && !listHlsSetNum.contains( rs.getString("set_num") ) ) {
									listHlsSetNum.add( rs.getString("set_num") );
								}
								
								String strEqualRtspCid = rs.getString("equal_rtsp_cid");
								String strCid = rs.getString("cid");
								String strFramesize = rs.getString("framesize");
								String strType = rs.getString("type");																										
								Integer pid = (Integer)rs.getObject("profile_id");
								long lExtraStatus = rs.getLong("extra_status");
								long lSid = rs.getLong("sid");
								if( (lExtraStatus & ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get()) == 0 || rs.wasNull() ) {
									lSid = -1;
								}
								
								String strStatus = rs.getString("status");
								if( strStatus == null || !strStatus.equals("[원본]작업대기") ) {
//									logger.info( String.format("Set Member Status is Not [원본]작업대기 | [ HLS ] [ %s ] [ MediaID : %s ] [ CID : %s ] [ Framesize : %s ]", 
//											strStatus,
//											rs.getString("mda_id"),
//											strCid,
//											strFramesize															
//											) );
									continue;
								}
																
								Define.IDItem idItem = new Define.IDItem( item.strMediaID, strCid, Define.IDType.HLS);																								
																
								if( listCheckerID.contains( strCid ) ) {
									continue;
								}
								
								logger.info( String.format( "Enc Get Assign Only HLS | [ MediaID : %s ] [ cid : %s ]", item.strMediaID, strCid ) );
																	
//								try {
//									do {
//										ProfileAssignInfo.ProfileInfo proInfo = ProfileAssignInfo.getProfileType(conn, pid);
//										nAssignTemp = ProfileAssignInfo.getAssign(conn, proInfo.type, proInfo.resolution, proInfo.strDivision);
//
//									}while( false );
//
//								}catch( Exception ex ) {
//									logger.error("",ex);
//								}
								
//								if( strEqualRtspCid != null && !strEqualRtspCid.isEmpty() ) {
//									nAssignTemp = 0;
//								}
								
								logger.info( String.format( "Enc Get Assign HLS TV | [ MediaID : %s ] [ cid : %s ] [ Framesize : %s ] [ Assign : %d ] [ strEqualRtspCid : %s ]", 		
															item.strMediaID,
															strCid,										
															strFramesize,
															nAssignTemp,
															strEqualRtspCid
															) );
								
								if( strType.toUpperCase().equals("TV") && ( strFramesize.toUpperCase().equals("UHD") || strFramesize.toUpperCase().equals("UHD+HDR") ) ) {
									ArrayList<Define.IDItem> listIdItem = new ArrayList<Define.IDItem>();
									listIdItem.add( idItem );
//									listTemp.add( new EncAssignItemEx( listIdItem, nAssignTemp, true, nScheTypeTV, lSid, false ) );
									listTemp.add( new EncAssignItemEx( listIdItem, true, nScheTypeTV, lSid, false ) );

								}else {							
									
									if( nAssignNoraml + nAssignTemp >= nEncMaxAssignVal ) {
										ArrayList<Define.IDItem> listIdItem = new ArrayList<Define.IDItem>();
										listIdItem.add( idItem );
//										listTemp.add( new EncAssignItemEx( 	listIdItem, nAssignTemp, false, strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile, lSid, false) );
										listTemp.add( new EncAssignItemEx( 	listIdItem, false, strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile, lSid, false) );
									}else {
										listCidsNormal.add( idItem );									
//										nAssignNoraml += nAssignTemp;
										lSidNoraml = lSid;
										nScheType = ( nScheType | nScheTypeTV );
									}
									
								}	
								
								listCheckerID.add( strCid );
							}							
						}						
					}
					
					// RTSP 동일 set_num 확인					
					sql = "SELECT * FROM cems.content WHERE encd_piqu_typ_cd=1 AND set_num=? AND ((extra_status & ?) = 0) and hls_yn is null and encd_piqu_typ_cd=?";
					try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
						for( String strSetNum : listHlsSetNum ) {
							int args = 1;
							pstmt.setString(args++, strSetNum );
							pstmt.setInt(args++, ExtraStatusManipulation.Flag.EF_NO_DISPLAY.get() );
							pstmt.setInt(args++, 1 );
							try( ResultSet rs = pstmt.executeQuery() ){												
								while (rs.next()) {											
									int nAssignTemp = 0;				
									String strPossnYn = rs.getString("possn_yn");
									String strcid = rs.getString("cid");
									String strFramesize = rs.getString("framesize");
									String strType = rs.getString("type");
									long lExtraStatus = rs.getLong("extra_status");
									boolean bIsVituralCid = ( lExtraStatus & lExtraStatusMix ) == 0 ? false : true;
									Integer pid = (Integer)rs.getObject("profile_id");
									long lSid = rs.getLong("sid");
									if( (lExtraStatus & ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get()) == 0 || rs.wasNull() ) {
										lSid = -1;
									}
									
									String strStatus = rs.getString("status");
									if( strStatus == null || !strStatus.equals("[원본]작업대기") ) {
//										logger.info( String.format("Set Member Status is Not [원본]작업대기 | [ RTSP ] [ %s ] [ CID : %s ] [ Type : %s ] [ Framesize : %s ] [ PossnYn : %s ]", 
//												strStatus,
//												strcid,
//												strType,
//												strFramesize,
//												rs.getString("possn_yn")
//												) );
										continue;
									}
									
									Define.IDItem idItem = new Define.IDItem(strcid, Define.IDType.RTSP);									
																																	
									if( listCheckerID.contains( strcid ) ) {
										continue;
									}
																					
//									try {
//										do {
//											ProfileAssignInfo.ProfileInfo proInfo = ProfileAssignInfo.getProfileType(conn, pid);
//											nAssignTemp = ProfileAssignInfo.getAssign(conn, proInfo.type, proInfo.resolution, proInfo.strDivision);
//										}while( false );
//
//									}catch( Exception ex ) {
//										logger.error("",ex);
//									}
									
									
									logger.info( String.format( "Enc Get Assign SetNum | [ set num : %s ] [ idType : %s ] [ cid : %s ] [ isVitural : %s ] [ Assign : %d ]", 
											strSetNum,
											idItem.eIDType.toString(),
											idItem.strID,
											bIsVituralCid ? "True" : "False",
											nAssignTemp
											) );
																												
									if( strType.toUpperCase().equals("TV") && ( strFramesize.toUpperCase().equals("UHD") || strFramesize.toUpperCase().equals("UHD+HDR") ) ) {										
										ArrayList<Define.IDItem> listIdItem = new ArrayList<Define.IDItem>();
										listIdItem.add( idItem );
//										listTemp.add( new EncAssignItemEx( listIdItem, nAssignTemp, true, nScheTypeTV, lSid, false ) );
										listTemp.add( new EncAssignItemEx( listIdItem, true, nScheTypeTV, lSid, false ) );

									}else {					
										if( nAssignNoraml + nAssignTemp >= nEncMaxAssignVal ) {
											ArrayList<Define.IDItem> listIdItem = new ArrayList<Define.IDItem>();
											listIdItem.add( idItem );
//											listTemp.add( new EncAssignItemEx( 	listIdItem, nAssignTemp, false, strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile, lSid, false) );
											listTemp.add( new EncAssignItemEx( 	listIdItem, false, strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile, lSid, false) );
										}else {
											listCidsNormal.add( idItem );
//											nAssignNoraml += nAssignTemp;
											lSidNoraml = lSid;
											nScheType = ( nScheType | ( strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile ) );
										}
																							
									}	
									
									listCheckerID.add( strcid );
								}
							}																	
						}							
					}					
				}
								
			}catch( Exception ex ){
				logger.error("",ex);				
			} finally {
				db.close();
			}
			
			if( listCidsNormal.size() > 0 ) {
				logger.info( String.format("Normal Assign Result : %d", nAssignNoraml ) );
//				EncAssignItemEx encAssignItem = new EncAssignItemEx( listCidsNormal, nAssignNoraml, false, nScheType, lSidNoraml, false );
				EncAssignItemEx encAssignItem = new EncAssignItemEx( listCidsNormal, false, nScheType, lSidNoraml, false );
				listTemp.add( encAssignItem );
			}
			
			listRet = listTemp;
			
		}catch( Exception ex ) {
			logger.error("",ex);
		}
		
		return listRet;
	}
	
	
	protected ArrayList<EncAssignItemEx> getAssign( ReadyItem item ) 
	{				
		ArrayList<EncAssignItemEx> listRet = new ArrayList<>();
		ArrayList<EncAssignItemEx> listTemp = new ArrayList<>();
		ArrayList<EncAssignItemEx> listRetry = new ArrayList<>();
		ArrayList<String> listCheckerID = new ArrayList<>();
		
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		
		ArrayList<Define.IDItem> listCidsRetry = new ArrayList<>();
		ArrayList<Define.IDItem> listCidsNormal = new ArrayList<>();			// RTSP Etc + HLS Etc
		long lSidNoraml = -1;
		long lSidRetry = -1;
		int nAssignRetry = 0;
		int nAssignNoraml = 0;				
		int nScheType = 0;
		int nScheTypeRetry = 0;
		long lExtraStatusMix = ( ExtraStatusManipulation.Flag.EF_NEW_DRM.get() | ExtraStatusManipulation.Flag.EF_WATERMARK.get() | ExtraStatusManipulation.Flag.EF_ADD_ECDN_TASK.get() );
		
		try {
			
			try {
				conn = db.startAutoCommit();			
				String sql = null;			
				
				HashMap< String, String > mapFramesizeTv = new DbCommonHelper().GetMapDbDefineCode(conn, Define.MappingDefineCodeGrpCd.RSLU_CODE_TV.toString() );
				if( mapFramesizeTv == null || mapFramesizeTv.isEmpty() ) {
					throw new Exception( String.format("Db Mapping Define Code Tv Rslu Data is Empty | [ %s ]", Define.MappingDefineCodeGrpCd.RSLU_CODE_TV.toString() ) );
				}
				
				if( item.eIDType == Define.IDType.RTSP ) {
					
					sql = "SELECT * FROM cems.content WHERE cid=?";
					try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
						pstmt.setString(1, item.cid );						
						try( ResultSet rs = pstmt.executeQuery() ){
							while (rs.next()) {
								String setPossnYN = rs.getString("possn_yn");						
								String setNum = rs.getString("set_num");
								if (setNum != null && setNum.length() > 0) {
									
									//RTSP
									sql = "SELECT * FROM cems.content WHERE encd_piqu_typ_cd IN ( 0, 2 ) AND set_num=? AND ((extra_status & ?) = 0) and hls_yn is null";
//									encd_piqu_typ_cd 1 -> supernova
									try (PreparedStatement pstmt2 = conn.prepareStatement(sql)) {
										pstmt2.setString(1, setNum);
										pstmt2.setInt(2, ExtraStatusManipulation.Flag.EF_NO_DISPLAY.get() );										
										logger.info( pstmt2.toString() );
										try( ResultSet rs2 = pstmt2.executeQuery() ){					
											while (rs2.next()) {											
												int nAssignTemp = 0;
												int encd_qlty_res_cd = rs2.getInt( "encd_qlty_res_cd" );
												String strPossnYn = rs2.getString("possn_yn");
												String strcid = rs2.getString("cid");
												String strFramesize = rs2.getString("framesize");
												String strType = rs2.getString("type");
												long lExtraStatus = rs2.getLong("extra_status");
												boolean bIsVituralCid = ( lExtraStatus & lExtraStatusMix ) == 0 ? false : true;
//												Integer pid = (Integer)rs2.getObject("profile_id");
												long lSid = rs2.getLong("sid");
												if( (lExtraStatus & ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get()) == 0 || rs2.wasNull() ) {
													lSid = -1;
												}
												
												String strStatus = rs2.getString("status");
												if( strStatus == null || !strStatus.equals("[원본]작업대기") ) {
//													logger.info( String.format("Set Member Status is Not [원본]작업대기 | [ RTSP ] [ %s ] [ CID : %s ] [ Type : %s ] [ Framesize : %s ] [ PossnYn : %s ]", 
//															strStatus,
//															strcid,
//															strType,
//															strFramesize,
//															rs2.getString("possn_yn")
//															) );
													continue;
												}
												
												Define.IDItem idItem = new Define.IDItem(strcid, Define.IDType.RTSP);												
																																				
												if( listCheckerID.contains( strcid) ) {
													continue;
												}

												// 인코더 할당 방삭 변경으로 인한 주석
//												try {
//													do {
//														ProfileAssignInfo.ProfileInfo proInfo = ProfileAssignInfo.getProfileType(conn, pid);
//														nAssignTemp = ProfileAssignInfo.getAssign(conn, proInfo.type, proInfo.resolution, proInfo.strDivision );
//													}while( false );
//
//												}catch( Exception ex ) {
//													logger.error("",ex);
//												}
												
												
												logger.info( String.format( "Enc Get Assign SetNum | [ set num : %s ] [ IDType : %s ] [ cid : %s ] [ isVitural : %s ] [ Assign : %d ] [ PossnYn : %s ]", 
														setNum,
														idItem.eIDType.toString(),
														idItem.strID,
														bIsVituralCid ? "True" : "False",
														nAssignTemp,
														strPossnYn
														) );
												if( (lExtraStatus & ExtraStatusManipulation.Flag.EF_REPLACE_JOB.get()) != 0 && encd_qlty_res_cd == Define.EncdQltyResCd.RetryRequet.get() ){
													
													listCidsRetry.add( idItem );
//													nAssignRetry += nAssignTemp; // 인코더 할당 방삭 변경으로 인한 주석
													lSidRetry = lSid;														
													nScheTypeRetry = ( nScheType | ( strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile ) );
													
												}else if( strType.toUpperCase().equals("TV") && ( strFramesize.toUpperCase().equals("UHD") || strFramesize.toUpperCase().equals("UHD+HDR") ) ) {													
													ArrayList<Define.IDItem> listIdItem = new ArrayList<Define.IDItem>();
													listIdItem.add( idItem );
													// 할당할 점수가 없음.. 새로운 생성자
//													listTemp.add( new EncAssignItemEx( listIdItem, nAssignTemp, true, nScheTypeTV, lSid, false ) );
													listTemp.add( new EncAssignItemEx( listIdItem, true, nScheTypeTV, lSid, false ) );

												}else {																																							
													listCidsNormal.add( idItem );
//													nAssignNoraml += nAssignTemp;
													lSidNoraml = lSid;														
													nScheType = ( nScheType | ( strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile ) );
												}											
												
												listCheckerID.add( strcid );
												
											}
										}
									}
									
									//HLS									
									sql = "SELECT * FROM cems.content_hls_enc WHERE set_num=?";
									try (PreparedStatement pstmt2 = conn.prepareStatement(sql)) {
										pstmt2.setString(1, setNum );																					
										logger.info( pstmt2.toString() );
										try( ResultSet rs2 = pstmt2.executeQuery() ){
											while( rs2.next() ) {
												
												int nAssignTemp = 0;
												int encd_qlty_res_cd = rs2.getInt("encd_qlty_res_cd");
												String strEqualRtspCid = rs2.getString("equal_rtsp_cid");
												String strcid = rs2.getString("cid");
												String strFramesize = rs2.getString("framesize");
												String strType = "TV";																														
												Integer pid = (Integer)rs2.getObject("profile_id");
												long lExtraStatus = rs2.getLong("extra_status");
												long lSid = rs2.getLong("sid");
												if( (lExtraStatus & ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get()) == 0 || rs2.wasNull() ) {
													lSid = -1;
												}
												
												Define.IDItem idItem = new Define.IDItem( rs2.getString("mda_id"), strcid, Define.IDType.HLS);												
												
												String strStatus = rs2.getString("status");
												if( strStatus == null || !strStatus.equals("[원본]작업대기") ) {
//													logger.info( String.format("Set Member Status is Not [원본]작업대기 | [ HLS ] [ %s ] [ MediaID : %s ] [ CID : %s ] [ Framesize : %s ]", 
//															strStatus,
//															rs2.getString("mda_id"),
//															strcid,
//															strFramesize															
//															) );
													continue;
												}
												
												if( listCheckerID.contains( strcid ) ) {
													continue;
												}																							

												// 인코더 할당 방식 변경으로 주석 처리
//												try {
//													do {
//														ProfileAssignInfo.ProfileInfo proInfo = ProfileAssignInfo.getProfileType(conn, pid);
//														nAssignTemp = ProfileAssignInfo.getAssign(conn, proInfo.type, proInfo.resolution, proInfo.strDivision );
//													}while( false );
//
//												}catch( Exception ex ) {
//													logger.error("",ex);
//												}
												
//												if( strEqualRtspCid != null && !strEqualRtspCid.isEmpty() ) {
//													nAssignTemp = 0;
//												}
												
												logger.info( String.format( "Enc Get Assign SetNum HLS | [ set num : %s ] [ MDA_RSLU_ID : %s ] [ Assign : %d ] [ strEqualRtspCid : %s ]", 
														setNum,
														strcid,																	
														nAssignTemp,
														strEqualRtspCid
														) );
												
												if( (lExtraStatus & ExtraStatusManipulation.Flag.EF_REPLACE_JOB.get()) != 0 && encd_qlty_res_cd == Define.EncdQltyResCd.RetryRequet.get() ){
													listCidsRetry.add( idItem );
//													nAssignRetry += nAssignTemp;
													lSidRetry = lSid;														
													nScheTypeRetry = ( nScheType | ( strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile ) );
													
												}else if( strType.toUpperCase().equals("TV") && ( strFramesize.toUpperCase().equals("UHD") || strFramesize.toUpperCase().equals("UHD+HDR") ) ) {																																						
													ArrayList<Define.IDItem> listIdItem = new ArrayList<Define.IDItem>();
													listIdItem.add( idItem );
//													listTemp.add( new EncAssignItemEx( listIdItem, nAssignTemp, true, nScheTypeTV, lSid, false ) );
													listTemp.add( new EncAssignItemEx( listIdItem, true, nScheTypeTV, lSid, false ) );

												}else {																										
													listCidsNormal.add( idItem );													
//													nAssignNoraml += nAssignTemp;
													lSidNoraml = lSid;
													nScheType = ( nScheType | nScheTypeTV );
																																		
												}		
												
												listCheckerID.add( strcid );
												
											}																										
										}
									}
																		
								} else if (rs.getString("type").toLowerCase().equals("mobile")) { // mobile only
									
									sql = "SELECT * FROM cems.content WHERE UPPER(type)='MOBILE' AND epsd_id= ? AND ((extra_status & ?) = 0) and status='[원본]작업대기' and possn_yn=? and hls_yn is null";
									try (PreparedStatement pstmt2 = conn.prepareStatement(sql)) {
										pstmt2.setString(1, item.strEpsdID );
										pstmt2.setInt(2, ExtraStatusManipulation.Flag.EF_NO_DISPLAY.get() );
										pstmt2.setString(3, setPossnYN);										
										logger.info( pstmt2.toString() );
										try( ResultSet rs2 = pstmt2.executeQuery() ){					
											while (rs2.next()) {										
												
												int nAssignTemp = 0;				
												int encd_qlty_res_cd = rs2.getInt("encd_qlty_res_cd");
												String strcid = rs2.getString("cid");
												String strFramesize = rs2.getString("framesize");
												String strType = rs2.getString("type");
												long lExtraStatus = rs2.getLong("extra_status");											
												boolean bIsVituralCid = ( lExtraStatus & lExtraStatusMix ) == 0 ? false : true;
												Integer pid = (Integer)rs2.getObject("profile_id");
												long lSid = rs2.getLong("sid");
												if((lExtraStatus & ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get()) == 0 ||  rs2.wasNull() ) {
													lSid = -1;
												}
												
												logger.info( String.format( "Enc Get Assign Mobile | [ CID : %s ]", strcid ) );
												
												String strStatus = rs2.getString("status");
												if( strStatus == null || !strStatus.equals("[원본]작업대기") ) {
//													logger.info( String.format("Set Member Status is Not [원본]작업대기 | [ RTSP ] [ %s ] [ CID : %s ] [ Type : %s ] [ Framesize : %s ] [ PossnYn : %s ]", 
//															strStatus,
//															strcid,
//															strType,
//															strFramesize,
//															rs2.getString("possn_yn")
//															) );
													continue;
												}
												
												Define.IDItem idItem = new Define.IDItem(strcid, Define.IDType.RTSP);												
												
												if( listCheckerID.contains( strcid ) ) {
													continue;
												}

												// 인코더 할당 방식 변경으로 인한 주석 처리
//												try {
//													do {
//														ProfileAssignInfo.ProfileInfo proInfo = ProfileAssignInfo.getProfileType(conn, pid);
//														nAssignTemp = ProfileAssignInfo.getAssign(conn, proInfo.type, proInfo.resolution, proInfo.strDivision);
//
//													}while( false );
//
//												}catch( Exception ex ) {
//													logger.error("",ex);
//												}
												
												logger.info( String.format( "Enc Get Assign Mobile | [ IDType : %s ] [ cid : %s ] [ isVitural : %s ] [ Assign : %d ]",
														idItem.eIDType.toString(),
														idItem.strID,
														bIsVituralCid ? "True" : "False",
														nAssignTemp
														) );
												
												if( (lExtraStatus & ExtraStatusManipulation.Flag.EF_REPLACE_JOB.get()) != 0 && encd_qlty_res_cd == Define.EncdQltyResCd.RetryRequet.get() ){
													listCidsRetry.add( idItem );
//													nAssignRetry += nAssignTemp;
													lSidRetry = lSid;														
													nScheTypeRetry = ( nScheType | ( strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile ) );													
												}else {
													listCidsNormal.add( idItem );
//													nAssignNoraml += nAssignTemp;
													lSidNoraml = lSid;
													nScheType = ( nScheType | nScheTypeMobile );																																																											
												}
												
												listCheckerID.add( strcid );
																										
												
											}
										}
									}
									
									
								}else {							
									// RTSP TV Only
									int nAssignTemp = 0;
									int encd_qlty_res_cd = rs.getInt("encd_qlty_res_cd");
									String strPossnYn = rs.getString("possn_yn");
									String strcid = rs.getString("cid");
									String strFramesize = rs.getString("framesize");
									String strType = rs.getString("type");
									long lExtraStatus = rs.getLong("extra_status");											
									boolean bIsVituralCid = ( lExtraStatus & lExtraStatusMix ) == 0 ? false : true;
									Integer pid = (Integer)rs.getObject("profile_id");
									long lSid = rs.getLong("sid");
									if( (lExtraStatus & ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get()) == 0 || rs.wasNull() ) {
										lSid = -1;
									}
									
									String strStatus = rs.getString("status");
									if( strStatus == null || !strStatus.equals("[원본]작업대기") ) {
//										logger.info( String.format("Set Member Status is Not [원본]작업대기 | [ RTSP ] [ %s ] [ CID : %s ] [ Type : %s ] [ Framesize : %s ] [ PossnYn : %s ]", 
//												strStatus,
//												strcid,
//												strType,
//												strFramesize,
//												rs.getString("possn_yn")
//												) );
										continue;
									}
									
									Define.IDItem idItem = new Define.IDItem(strcid, Define.IDType.RTSP);																																			
									
									if( listCheckerID.contains( strcid ) ) {
										continue;
									}
									
									logger.info( String.format( "Enc Get Assign Only One | [ IDType : %s ] [ CID : %s ]", idItem.eIDType.toString(), idItem.strID ) );
																		
//									try {
//										do {
//											ProfileAssignInfo.ProfileInfo proInfo = ProfileAssignInfo.getProfileType(conn, pid);
//											nAssignTemp = ProfileAssignInfo.getAssign(conn, proInfo.type, proInfo.resolution, proInfo.strDivision);
//
//										}while( false );
//
//									}catch( Exception ex ) {
//										logger.error("",ex);
//									}
									
									logger.info( String.format( "Enc Get Assign TV | [ IDType : %s ] [ cid : %s ] [ isVitural : %s ] [ Assign : %d ]",
											idItem.eIDType.toString(),
											idItem.strID,
											bIsVituralCid ? "True" : "False",
											nAssignTemp
											) );
																												
									
									if( (lExtraStatus & ExtraStatusManipulation.Flag.EF_REPLACE_JOB.get()) != 0 && encd_qlty_res_cd == Define.EncdQltyResCd.RetryRequet.get() ){
										listCidsRetry.add( idItem );
//										nAssignRetry += nAssignTemp;
										lSidRetry = lSid;														
										nScheTypeRetry = ( nScheType | ( strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile ) );
										
									}else if( strType.toUpperCase().equals("TV") && ( strFramesize.toUpperCase().equals("UHD") || strFramesize.toUpperCase().equals("UHD+HDR") ) ) {																			
										ArrayList<Define.IDItem> listIdItem = new ArrayList<Define.IDItem>();
										listIdItem.add( idItem );
//										listTemp.add( new EncAssignItemEx( listIdItem, nAssignTemp, true, nScheTypeTV, lSid, false ) );
										listTemp.add( new EncAssignItemEx( listIdItem, true, nScheTypeTV, lSid, false ) );

									}else {																				
										listCidsNormal.add( idItem );
//										nAssignNoraml += nAssignTemp;
										lSidNoraml = lSid;
										nScheType = ( nScheType | ( strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile ) );																		
										
									}	
									
									listCheckerID.add( strcid );
									
								}
							}
						}
					}
					
				}else if( item.eIDType == Define.IDType.HLS ) {
					
					ArrayList< String > listHlsSetNum = new ArrayList<>();
					
					sql = "SELECT * FROM cems.content_hls_enc WHERE mda_id=?";
					try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
						pstmt.setString(1, item.strMediaID );						
						try( ResultSet rs = pstmt.executeQuery() ){
							while( rs.next() ) {
							
								int nAssignTemp = 0;	
								if( rs.getString("set_num") != null && !rs.getString("set_num").isEmpty() && !listHlsSetNum.contains( rs.getString("set_num") ) ) {
									listHlsSetNum.add( rs.getString("set_num") );
								}
								
								int encd_qlty_res_cd = rs.getInt("encd_qlty_res_cd");
								String strEqualRtspCid = rs.getString("equal_rtsp_cid");
								String strCid = rs.getString("cid");
								String strFramesize = rs.getString("framesize");
								String strType = rs.getString("type");																										
								Integer pid = (Integer)rs.getObject("profile_id");
								long lExtraStatus = rs.getLong("extra_status");
								long lSid = rs.getLong("sid");
								if( (lExtraStatus & ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get()) == 0 || rs.wasNull() ) {
									lSid = -1;
								}
								
								String strStatus = rs.getString("status");
								if( strStatus == null || !strStatus.equals("[원본]작업대기") ) {
//									logger.info( String.format("Set Member Status is Not [원본]작업대기 | [ HLS ] [ %s ] [ MediaID : %s ] [ CID : %s ] [ Framesize : %s ]", 
//											strStatus,
//											rs.getString("mda_id"),
//											strCid,
//											strFramesize															
//											) );
									continue;
								}
																
								Define.IDItem idItem = new Define.IDItem( item.strMediaID, strCid, Define.IDType.HLS);																								
																
								if( listCheckerID.contains( strCid ) ) {
									continue;
								}
								
								logger.info( String.format( "Enc Get Assign Only HLS | [ MediaID : %s ] [ cid : %s ]", item.strMediaID, strCid ) );


								// 인코더 할당 방식 변경
//								try {
//									do {
//										ProfileAssignInfo.ProfileInfo proInfo = ProfileAssignInfo.getProfileType(conn, pid);
//										nAssignTemp = ProfileAssignInfo.getAssign(conn, proInfo.type, proInfo.resolution, proInfo.strDivision);
//
//									}while( false );
//
//								}catch( Exception ex ) {
//									logger.error("",ex);
//								}
								
//								if( strEqualRtspCid != null && !strEqualRtspCid.isEmpty() ) {
//									nAssignTemp = 0;
//								}
								
								logger.info( String.format( "Enc Get Assign HLS TV | [ MediaID : %s ] [ cid : %s ] [ Framesize : %s ] [ Assign : %d ] [ strEqualRtspCid : %s ]", 		
															item.strMediaID,
															strCid,										
															strFramesize,
															nAssignTemp,
															strEqualRtspCid
															) );
								
								if( (lExtraStatus & ExtraStatusManipulation.Flag.EF_REPLACE_JOB.get()) != 0 && encd_qlty_res_cd == Define.EncdQltyResCd.RetryRequet.get() ){
									listCidsRetry.add( idItem );
//									nAssignRetry += nAssignTemp;
									lSidRetry = lSid;														
									nScheTypeRetry = ( nScheType | ( strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile ) );
									
								}else if( strType.toUpperCase().equals("TV") && ( strFramesize.toUpperCase().equals("UHD") || strFramesize.toUpperCase().equals("UHD+HDR") ) ) {
									ArrayList<Define.IDItem> listIdItem = new ArrayList<Define.IDItem>();
									listIdItem.add( idItem );
//									listTemp.add( new EncAssignItemEx( listIdItem, nAssignTemp, true, nScheTypeTV, lSid, false ) );
									listTemp.add( new EncAssignItemEx( listIdItem, true, nScheTypeTV, lSid, false ) );

								}else {																	
									listCidsNormal.add( idItem );									
//									nAssignNoraml += nAssignTemp;
									lSidNoraml = lSid;
									nScheType = ( nScheType | nScheTypeTV );																	
									
								}	
								
								listCheckerID.add( strCid );
							}							
						}						
					}
					
					// RTSP 동일 set_num 확인					
					sql = "SELECT * FROM cems.content WHERE encd_piqu_typ_cd IN ( 0, 2 ) AND set_num=? AND ((extra_status & ?) = 0) and hls_yn is null";
					try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
						for( String strSetNum : listHlsSetNum ) {
							int args = 1;
							pstmt.setString(args++, strSetNum );
							pstmt.setInt(args++, ExtraStatusManipulation.Flag.EF_NO_DISPLAY.get() );							
							try( ResultSet rs = pstmt.executeQuery() ){												
								while (rs.next()) {											
									int nAssignTemp = 0;		
									int encd_qlty_res_cd = rs.getInt("encd_qlty_res_cd");
									String strPossnYn = rs.getString("possn_yn");
									String strcid = rs.getString("cid");
									String strFramesize = rs.getString("framesize");
									String strType = rs.getString("type");
									long lExtraStatus = rs.getLong("extra_status");
									boolean bIsVituralCid = ( lExtraStatus & lExtraStatusMix ) == 0 ? false : true;
									Integer pid = (Integer)rs.getObject("profile_id");
									long lSid = rs.getLong("sid");
									if( (lExtraStatus & ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get()) == 0 || rs.wasNull() ) {
										lSid = -1;
									}
									
									String strStatus = rs.getString("status");
									if( strStatus == null || !strStatus.equals("[원본]작업대기") ) {
//										logger.info( String.format("Set Member Status is Not [원본]작업대기 | [ RTSP ] [ %s ] [ CID : %s ] [ Type : %s ] [ Framesize : %s ] [ PossnYn : %s ]", 
//												strStatus,
//												strcid,
//												strType,
//												strFramesize,
//												rs.getString("possn_yn")
//												) );
										continue;
									}
									
									Define.IDItem idItem = new Define.IDItem(strcid, Define.IDType.RTSP);									
																																	
									if( listCheckerID.contains( strcid ) ) {
										continue;
									}

									// 인코딩 할당 방식 변경
//									try {
//										do {
//											ProfileAssignInfo.ProfileInfo proInfo = ProfileAssignInfo.getProfileType(conn, pid);
//											nAssignTemp = ProfileAssignInfo.getAssign(conn, proInfo.type, proInfo.resolution, proInfo.strDivision);
//										}while( false );
//
//									}catch( Exception ex ) {
//										logger.error("",ex);
//									}
									
									
									logger.info( String.format( "Enc Get Assign SetNum | [ set num : %s ] [ idType : %s ] [ cid : %s ] [ isVitural : %s ] [ Assign : %d ]", 
											strSetNum,
											idItem.eIDType.toString(),
											idItem.strID,
											bIsVituralCid ? "True" : "False",
											nAssignTemp
											) );
																												
									if( (lExtraStatus & ExtraStatusManipulation.Flag.EF_REPLACE_JOB.get()) != 0 && encd_qlty_res_cd == Define.EncdQltyResCd.RetryRequet.get() ){
										listCidsRetry.add( idItem );
//										nAssignRetry += nAssignTemp;
										lSidRetry = lSid;														
										nScheTypeRetry = ( nScheType | ( strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile ) );
										
									}else if( strType.toUpperCase().equals("TV") && ( strFramesize.toUpperCase().equals("UHD") || strFramesize.toUpperCase().equals("UHD+HDR") ) ) {										
										ArrayList<Define.IDItem> listIdItem = new ArrayList<Define.IDItem>();
										listIdItem.add( idItem );
//										listTemp.add( new EncAssignItemEx( listIdItem, nAssignTemp, true, nScheTypeTV, lSid, false ) );
										listTemp.add( new EncAssignItemEx( listIdItem, true, nScheTypeTV, lSid, false ) );

									}else {																														
										listCidsNormal.add( idItem );
//										nAssignNoraml += nAssignTemp;
										lSidNoraml = lSid;
										nScheType = ( nScheType | ( strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile ) );																		
																							
									}	
									
									listCheckerID.add( strcid );
								}
							}																	
						}							
					}					
				}
								
			}catch( Exception ex ){
				logger.error("",ex);				
			} finally {
				db.close();
			}
			
			if( listCidsNormal.size() > 0 ) {
				logger.info( String.format("Normal Assign Result : %d", nAssignNoraml ) );
//				EncAssignItemEx encAssignItem = new EncAssignItemEx( listCidsNormal, nAssignNoraml, false, nScheType, lSidNoraml, false );
				EncAssignItemEx encAssignItem = new EncAssignItemEx( listCidsNormal, false, nScheType, lSidNoraml, false );
				listTemp.add( encAssignItem );
			}
			
			if( listCidsRetry.size() > 0 ) {
				logger.info( String.format("Retry Assign Result : %d", nAssignRetry ) );
//				EncAssignItemEx encAssignItem = new EncAssignItemEx( listCidsRetry, nAssignRetry, false, nScheTypeRetry, lSidRetry, true );
				EncAssignItemEx encAssignItem = new EncAssignItemEx( listCidsRetry, false, nScheTypeRetry, lSidRetry, true );
				listTemp.add( encAssignItem );
			}
			
			listRet = listTemp;
			
		}catch( Exception ex ) {
			logger.error("",ex);
		}
		
		return listRet;
	}
	

	protected HashMap<Long, Integer> RenewalMapTempEncAssign( HashMap<Long, Integer> mapTempEncAssign, Long lSid, int nAssign ) throws Exception
	{
		HashMap<Long, Integer> mapRet = mapTempEncAssign;
		
		try {
			if( mapRet == null ) {
				throw new Exception( "mapTempEncAssign is null" );
			}
			
			Integer nAlreadyAssign = mapRet.get( lSid );
			if( nAlreadyAssign == null ) {
				mapRet.put( lSid, nAssign );
			}else {
				mapRet.put( lSid, nAlreadyAssign + nAssign );
			}
			
		}catch( Exception ex ) {
			logger.error("",ex);
			throw ex;
		}
		
		return mapRet;
	}
}
