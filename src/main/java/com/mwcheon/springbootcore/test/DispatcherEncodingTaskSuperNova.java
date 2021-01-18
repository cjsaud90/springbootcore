package com.mwcheon.springbootcore.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import common.DbCommonHelper;
import common.DbTransaction;
import common.Define;
import common.EmsException;
import common.ExtraStatusManipulation;
import common.ProfileAssignInfo;
import common.Util;
import common.WatermarkDrmInfo;
import common.WhoAmI;
import common.Util.contIDInfo;
import common.system.LoadBalancerEncoderElemental;
import common.system.LoadBalancerEncoderSuperNova;
import controller.BroadcastTaskManager;
import controller.RawTaskSchedule;
import controller.TranscodingStatus;
import controller.TranscodingTask;
import controller.TsTaskSchedule;
import task.TaskProcessorManager;
import task.pipeline.MobileEncodingPipeline;
import task.pipeline.Pipeline;
import task.pipeline.TvEncodingPipeline;
import task.pipeline.TvMobileEncodingPipeline;

public class DispatcherEncodingTaskSuperNova extends DispatcherEncodingTaskBase  {
	private static Logger logger = LoggerFactory.getLogger(DispatcherEncodingTaskSuperNova.class);	
		
	@Override
	public void run() {
		logger.info("running...");
		while (true) {
			try {
				sleep(2000);
				
				if (WhoAmI.isMaster() == false && WhoAmI.isMainAlive()) {
					continue;
				}
				
				// get items to encoding
				// 작업예약된 항목부터 배열에 저장된다.
				ArrayList<ReadyItem> items = getReadyItem();
				if (items.isEmpty()) {
					sleep(2000);
					continue;
				}
				
				int maxAssign = 1300;
				try {
					String strMaxAssign = new DbCommonHelper().GetConfigureDBValue(null, "enc_max_assign", false );
					if( strMaxAssign != null && common.Util.isNumber( strMaxAssign ) ) {
						maxAssign = Integer.valueOf( strMaxAssign );
					}	
				}catch( Exception ex ) {}		
				
				ArrayList<EncAssignItemEx> listRetEncAssignItem = new ArrayList<DispatcherEncodingTaskSuperNova.EncAssignItemEx>();
								
				for (ReadyItem item : items) {
					
					ArrayList<EncAssignItemEx> listEncAssignItem = getAssignBySuperNova(item, maxAssign );
					if( listEncAssignItem == null || listEncAssignItem.size() <= 0 ){
						continue;
					}				
					
					HashMap< Long, Integer> mapTempEncAssign = new HashMap<>();
					
					for( EncAssignItemEx encAssignItem : listEncAssignItem ) {
						
						boolean bUseTV = false;
						boolean bUseMobile = false;
						
						if( ( encAssignItem.nScheType & nScheTypeTV ) != 0  ) {
							bUseTV = true;
						}else if( ( encAssignItem.nScheType & nScheTypeMobile ) != 0 ) {
							bUseMobile = true;
						}
						
						if( encAssignItem.lSid != null && encAssignItem.lSid > 0 ) {
							logger.info( "====================================================");
							logger.info( String.format("SuperNova Encoder Reseved Setting Start | [ CID : %s ] [ SID : %d ]", item.cid, encAssignItem.lSid.longValue() ) );
							logger.info( "====================================================");
							Map<String, Object> result = LoadBalancerEncoderSuperNova.available(encAssignItem.listCids, encAssignItem.lSid.longValue(), item.hasUhd );
							if( (boolean) result.get("result") ) {
								encAssignItem.lSid = item.sid;							
								mapTempEncAssign = RenewalMapTempEncAssign( mapTempEncAssign, encAssignItem.lSid, (Integer) result.get("result"));
							}else {
								encAssignItem.lSid = (long)-1;
							}
							
						}else {

							Map<String, Object> result  = LoadBalancerEncoderSuperNova.available( encAssignItem.listCids, encAssignItem.bIsUHD, bUseTV, bUseMobile, false, mapTempEncAssign );
							long nRetSid = (long) result.get("sid");
							int nAssign = (int) result.get("nAssign");

							if( nRetSid > -1 ) {
								encAssignItem.lSid = nRetSid;						
								mapTempEncAssign = RenewalMapTempEncAssign( mapTempEncAssign, encAssignItem.lSid, nAssign );
							}
							
						}
						
					}
										
					for( EncAssignItemEx encAssignItem : listEncAssignItem ) {
						if( encAssignItem.lSid != null && encAssignItem.lSid > 0 ) {
							listRetEncAssignItem.add( encAssignItem );							
						}						
					}
					
					if( listRetEncAssignItem.size() > 0 ) {
						//인코더 할당 
						for( EncAssignItemEx encAssignItem : listRetEncAssignItem ) {
							LoadBalancerEncoderElemental.getSystem(encAssignItem.lSid, encAssignItem.nAssign);
							logger.info("");
							logger.info("=======================================================================");
							logger.info(String.format("SuperNova Enc Assign OK | [ SID : %d ] [ Assign : %d ]", encAssignItem.lSid, encAssignItem.nAssign ));
							for( Define.IDItem idItem : encAssignItem.listCids ) {
								logger.info( String.format("[ IDType : %s ] [ ID : %s ] [ MediaID : %s ]", idItem.eIDType.toString(), idItem.strID, idItem.strHlsMediaID ) );
							}
							logger.info("=======================================================================");
						}												
						break;
					}											
				}				
				
				if( listRetEncAssignItem == null || listRetEncAssignItem.isEmpty() ) {
					sleep(1000); // 사용자가 편성항목에 대해 상태를 변경할 수 있기에 잠시 쉬었다 다시 조회한다. (ex HOLD)
					continue;
				}
								
				// [원본]작업대기 상태로 변경 
				setWorkingStatusEx( listRetEncAssignItem );
				
				// create working pipeline
				for( EncAssignItemEx encAssignItem : listRetEncAssignItem ) {
					// 할당량이 0 이하 ( UHD 인코딩완료파일 복사 )인 경우는 Pipeline을 만들지 않는다.
					if( encAssignItem.nAssign <= 0 ) {
						continue;
					}
					encAssignItem.pipeline = createPipeline( encAssignItem.nScheType );
					
					for( Define.IDItem idItem : encAssignItem.listCids ) {
						logger.info( "execute [ ID Type : " + idItem.eIDType.toString() + " | ID : " + idItem.strID + "] -> task-processor[" + encAssignItem.pipeline + "]");	
					}				
				}
				
				// launch task
				for( EncAssignItemEx encAssignItem : listRetEncAssignItem ) {
					if( encAssignItem.pipeline == null ) {
						continue;
					}
					
					logger.info( String.format("TaskProcessorManager Run Param | [ Pipeline : %s ] [ Assign : %d ] [ CID Cnt : %d ]", 							
							encAssignItem.pipeline,  
							encAssignItem.nAssign,
							encAssignItem.listCids.size()
							) );
					
					TaskProcessorManager.run(null, encAssignItem.pipeline, encAssignItem.nAssign, encAssignItem.listCids );	
				}
				
			} catch (InterruptedException e) {
				logger.error("", e);
				break;
			} catch (Exception e) {
				logger.error("", e);
			}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
		}		
	}
	
	protected ArrayList<ReadyItem> getReadyItem() {
		ArrayList<ReadyItem> items = new ArrayList<>();
		ArrayList<ReadyItem> itemsNorm = new ArrayList<>();
		ArrayList<ReadyItem> itemsResv = new ArrayList<>(); // 작업예약		
		//HLS
		ArrayList<ReadyItem> itemsHls = new ArrayList<>(); // HLS
		
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		try {
			conn = db.startAutoCommit();
			String sql = null;
			
			HashMap<String, String> mapFramesize = new DbCommonHelper().GetMapDbDefineCode(conn, Define.MappingDefineCodeGrpCd.RSLU_CODE_TV.toString() );
			if( mapFramesize == null || mapFramesize.isEmpty() ) {
				throw new Exception( String.format("Db Mapping Deinfe Code Map Data is Empty | [ %s ]", Define.MappingDefineCodeGrpCd.RSLU_CODE_TV.toString() ) );
			}
			
			// 편성일 기준 RTSP
			sql = "SELECT * FROM content WHERE encd_piqu_typ_cd=1 and ( hls_yn is null or hls_yn='' ) and status='[원본]작업대기' AND (extra_status & ?)=0 ORDER BY last_time asc, schedule_time ASC";
			try (PreparedStatement pstmt = conn.prepareStatement(sql) ) {
				pstmt.setLong(1, ( ExtraStatusManipulation.Flag.EF_HOLD_ON.get() | ExtraStatusManipulation.Flag.EF_NEW_DRM.get() | ExtraStatusManipulation.Flag.EF_WATERMARK.get() | ExtraStatusManipulation.Flag.EF_ADD_ECDN_TASK.get() ) );
				try( ResultSet rs = pstmt.executeQuery() ){
					while (rs.next()) {
						ReadyItem item = new ReadyItem();
						item.eIDType = Define.IDType.RTSP;
						item.strEpsdID = rs.getString("epsd_id");
						item.cid = rs.getString("cid");						
						item.type = rs.getString("type");
						item.frameSize = rs.getString("framesize");
						item.hasUhd = false;
						int extraStatus = rs.getInt("extra_status");
						
						if ((extraStatus & ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get()) != 0) {
							long lsid = rs.getLong("sid");
							//logger.info( String.format( "found reserved job! Get sid : %d", lsid ) );
							item.sid = lsid > 0 ? lsid : null;
							//logger.info("found reserved job! cid=" + item.cid + ", sid=" + item.sid);
							itemsResv.add(item);
						} else {
							itemsNorm.add(item);
						}
						
					}
				}
			}
			
			// insert_data ? HLS			
			sql = "SELECT * FROM content_hls_enc WHERE encd_piqu_typ_cd=1 and status='[원본]작업대기' AND (extra_status & ?)=0  ORDER BY insert_time ASC, last_time asc";
			try (PreparedStatement pstmt = conn.prepareStatement(sql) ) {
				pstmt.setLong(1, ( ExtraStatusManipulation.Flag.EF_NO_DISPLAY.get() | ExtraStatusManipulation.Flag.EF_HOLD_ON.get() ) );
				try( ResultSet rs = pstmt.executeQuery() ){
					while (rs.next()) {
						ReadyItem item = new ReadyItem();
						item.eIDType = Define.IDType.HLS;
						item.strMediaID = rs.getString("mda_id");
						item.cid = rs.getString("cid");												
						item.frameSize = rs.getString("framesize");
						item.hasUhd = false;						
						long lsid = rs.getLong("sid");
						//logger.info( String.format( "found reserved job! Get sid : %d", lsid ) );
						item.sid = lsid > 0 ? lsid : null;
						itemsHls.add( item );						
					}
				}
			}			
			
			if (itemsResv.isEmpty() == false) {
				items.addAll(itemsResv);				
			}	
			
			items.addAll(itemsNorm);
			
			if (itemsHls.isEmpty() == false) {
				items.addAll(itemsHls);				
			}
			
			//logger.debug("작업대기 항목 : " + items.size());
			return items;
			
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			db.close();
		}
		return null;
	}
	
	// EncPipeline에 할당 할 CIDs 및 Assign 값을 가지고 온다	
//	protected ArrayList<EncAssignItemEx> getAssign( ReadyItem item ) 
//	{				
//		ArrayList<EncAssignItemEx> listRet = new ArrayList<>();
//		ArrayList<EncAssignItemEx> listTemp = new ArrayList<>();
//		
//		DbTransaction db = new DbTransaction();
//		Connection conn = null;
//		
//		ArrayList<Define.IDItem> listCidsNormal = new ArrayList<>();		
//		long lExtraStatusMix = ( ExtraStatusManipulation.Flag.EF_NEW_DRM.get() | ExtraStatusManipulation.Flag.EF_WATERMARK.get() | ExtraStatusManipulation.Flag.EF_ADD_ECDN_TASK.get() );
//		boolean bUHD = false;
//		try {
//			
//			try {
//				conn = db.startAutoCommit();
//				String sql = null;			
//				
//				HashMap< String, String > mapFramesizeTv = new DbCommonHelper().GetMapDbDefineCode(conn, Define.MappingDefineCodeGrpCd.RSLU_CODE_TV.toString() );
//				if( mapFramesizeTv == null || mapFramesizeTv.isEmpty() ) {
//					throw new Exception( String.format("Db Mapping Define Code Tv Rslu Data is Empty | [ %s ]", Define.MappingDefineCodeGrpCd.RSLU_CODE_TV.toString() ) );
//				}
//				
//				if( item.eIDType == Define.IDType.RTSP ) {
//					
//					sql = "SELECT * FROM cems.content WHERE cid=?";
//					try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
//						pstmt.setString(1, item.cid );
//						try( ResultSet rs = pstmt.executeQuery() ){
//							if(rs.next()) {
//								
//								int nAssignTemp = 0;								
//								String strcid = rs.getString("cid");
//								String strFramesize = rs.getString("framesize");
//								String strType = rs.getString("type");
//								long lExtraStatus = rs.getLong("extra_status");											
//								boolean bIsVituralCid = ( lExtraStatus & lExtraStatusMix ) == 0 ? false : true;
//								Integer pid = (Integer)rs.getObject("profile_id");
//								long lSid = rs.getLong("sid");
//								if( (lExtraStatus & ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get()) == 0 || rs.wasNull() ) {
//									lSid = -1;
//								}
//
//								Define.IDItem idItem = new Define.IDItem(strcid, Define.IDType.RTSP);																																																			
//								
//								logger.info( String.format( "Enc Get Assign Only One | [ IDType : %s ] [ CID : %s ]", idItem.eIDType.toString(), idItem.strID ) );
//																	
//								try {
//									do {										
//										ProfileAssignInfo.ProfileInfo proInfo = ProfileAssignInfo.getProfileType(conn, pid);
//										nAssignTemp = ProfileAssignInfo.getAssign(conn, proInfo.type, proInfo.resolution, proInfo.strDivision );
//										
//									}while( false );											
//										
//								}catch( Exception ex ) {
//									logger.error("",ex);
//								}																			
//								
//								logger.info( String.format( "Enc Get Assign TV | [ IDType : %s ] [ cid : %s ] [ isVitural : %s ] [ Assign : %d ]",
//										idItem.eIDType.toString(),
//										idItem.strID,
//										bIsVituralCid ? "True" : "False",
//										nAssignTemp
//										) );
//								
//								if( strType.toUpperCase().equals("TV") && ( strFramesize.toUpperCase().equals("UHD") || strFramesize.toUpperCase().equals("UHD+HDR") ) ) {																											
//									bUHD = true;
//								}
//								
//								listCidsNormal.add( idItem );
//								EncAssignItemEx encAssignItem = new EncAssignItemEx( listCidsNormal, nAssignTemp, bUHD, strType.toUpperCase().equals("TV") ?  nScheTypeTV : nScheTypeMobile, lSid );	
//								listTemp.add( encAssignItem );
//							}							
//						}
//					}
//					
//				}else if( item.eIDType == Define.IDType.HLS ) {
//										
//					sql = "SELECT * FROM cems.content_hls_enc WHERE cid=?";
//					try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
//						pstmt.setString(1, item.cid );
//						try( ResultSet rs = pstmt.executeQuery() ){
//							if( rs.next() ) {
//							
//								int nAssignTemp = 0;													
//								String strCid = rs.getString("cid");
//								String strFramesize = rs.getString("framesize");
//								String strType = rs.getString("type");
//								Integer pid = (Integer)rs.getObject("profile_id");
//								long lExtraStatus = rs.getLong("extra_status");
//								long lSid = rs.getLong("sid");
//								if( (lExtraStatus & ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get()) == 0 || rs.wasNull() ) {
//									lSid = -1;
//								}
//								
//								Define.IDItem idItem = new Define.IDItem( item.strMediaID, strCid, Define.IDType.HLS);																								
//								
//								logger.info( String.format( "Enc Get Assign Only HLS | [ MediaID : %s ] [ cid : %s ]", item.strMediaID, strCid ) );
//																	
//								try {
//									do {										
//										ProfileAssignInfo.ProfileInfo proInfo = ProfileAssignInfo.getProfileType(conn, pid);
//										nAssignTemp = ProfileAssignInfo.getAssign(conn, proInfo.type, proInfo.resolution, proInfo.strDivision );
//										
//									}while( false );											
//										
//								}catch( Exception ex ) {
//									logger.error("",ex);
//								}													
//								
//								logger.info( String.format( "Enc Get Assign HLS TV | [ MediaID : %s ] [ cid : %s ] [ Framesize : %s ] [ Assign : %d ]", 		
//															item.strMediaID,
//															strCid,										
//															strFramesize,
//															nAssignTemp															
//															) );
//								
//								if( strType.toUpperCase().equals("TV") && ( strFramesize.toUpperCase().equals("UHD") || strFramesize.toUpperCase().equals("UHD+HDR") ) ) {																											
//									bUHD = true;
//								}
//								
//								listCidsNormal.add( idItem );
//								EncAssignItemEx encAssignItem = new EncAssignItemEx( listCidsNormal, nAssignTemp, bUHD, nScheTypeTV, lSid );	
//								listTemp.add( encAssignItem );
//							}							
//						}						
//					}																				
//				}
//								
//			}catch( Exception ex ){
//				logger.error("",ex);				
//			} finally {
//				db.close();
//			}
//						
//			listRet = listTemp;
//			
//		}catch( Exception ex ) {
//			logger.error("",ex);
//		}
//		
//		return listRet;
//	}
	
	
}