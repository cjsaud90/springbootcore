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

public class DispatcherEncodingTaskElemental extends DispatcherEncodingTaskBase  {
	private static Logger logger = LoggerFactory.getLogger(DispatcherEncodingTaskElemental.class);	
	
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
											
				ArrayList<EncAssignItemEx> listRetEncAssignItem = new ArrayList<EncAssignItemEx>();
								
				for (ReadyItem item : items) {
					
					ArrayList<EncAssignItemEx> listEncAssignItem = getAssign( item );
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
							logger.info( String.format("Encoder Reseved Setting Start | [ CID : %s ] [ SID : %d ]", item.cid, encAssignItem.lSid.longValue() ) );
							logger.info( "====================================================");
							Map<String, Object> result = LoadBalancerEncoderElemental.available(encAssignItem.lSid.longValue(), encAssignItem.listCids, item.hasUhd, true, bUseTV, bUseMobile, encAssignItem.bRetry, mapTempEncAssign );

							if( (boolean) result.get("result") ) {
								encAssignItem.lSid = item.sid;
								mapTempEncAssign = RenewalMapTempEncAssign( mapTempEncAssign, encAssignItem.lSid, (Integer) result.get("nAssign"));
							}else {
								encAssignItem.lSid = (long)-1;
							}
							
						}else {
							Map<String, Object> result = LoadBalancerEncoderElemental.available( encAssignItem.listCids, encAssignItem.bIsUHD, true, bUseTV, bUseMobile, false, encAssignItem.bRetry, mapTempEncAssign );
							long nRetSid = (long) result.get("sid");
							int nAssign = (int) result.get("nAssign");

							if( nRetSid > -1 ) {
								encAssignItem.lSid = nRetSid;								
								mapTempEncAssign = RenewalMapTempEncAssign( mapTempEncAssign, encAssignItem.lSid, nAssign );
							}
							
						}
						
					}
					
					String strEncDispatcherMode = new DbCommonHelper().GetConfigureDBValue(null, "enc_dispatcher_mode_extension", false );
					if( strEncDispatcherMode != null && strEncDispatcherMode.equals("1") ) {
						
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
								logger.info(String.format("Enc Assign OK | [ SID : %d ] [ Assign : %d ]", encAssignItem.lSid, encAssignItem.nAssign ));
								for( Define.IDItem idItem : encAssignItem.listCids ) {
									logger.info( String.format("[ IDType : %s ] [ ID : %s ] [ MediaID : %s ]", idItem.eIDType.toString(), idItem.strID, idItem.strHlsMediaID ) );
								}
								logger.info("=======================================================================");
							}												
							break;
						}						
						
					}else {

						boolean bAssignAllOK = true;
						for( EncAssignItemEx encAssignItem : listEncAssignItem ) {
							if( encAssignItem.lSid == null || encAssignItem.lSid < 0 ) {
								bAssignAllOK = false;
								break;
							}						
						}
						
						if( bAssignAllOK ) {
							//인코더 할당 
							for( EncAssignItemEx encAssignItem : listEncAssignItem ) {
								LoadBalancerEncoderElemental.getSystem(encAssignItem.lSid, encAssignItem.nAssign);
								logger.info("=======================================================================");
								logger.info(String.format("Enc Assign OK | [ SID : %d ] [ Assign : %d ]", encAssignItem.lSid, encAssignItem.nAssign ));
								logger.info("=======================================================================");
							}						
							listRetEncAssignItem = listEncAssignItem;
							break;
						}
						
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
		ArrayList<ReadyItem> itemsER = new ArrayList<>(); // 긴급인코딩
		ArrayList<ReadyItem> itemsERResv = new ArrayList<>(); // 긴급인코딩 예약
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
			sql = "SELECT * FROM content WHERE encd_piqu_typ_cd IN ( 0, 2 ) and ( hls_yn is null or hls_yn='' ) and status='[원본]작업대기' AND (extra_status & ?)=0 ORDER BY schedule_time ASC";
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
						if ((extraStatus & ExtraStatusManipulation.Flag.EF_EMERGENCY_ENC.get()) != 0) {
							//긴급인코딩 OK!
							if ((extraStatus & ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get()) != 0) {
								long lsid = rs.getLong("sid");
								//logger.info( String.format( "found reserved job! Get sid : %d", lsid ) );
								item.sid = lsid > 0 ? lsid : null;
								//logger.info("found reserved job! cid=" + item.cid + ", sid=" + item.sid);
								itemsERResv.add(item);
							} else {
								itemsER.add(item);
							}
							
						}else{
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
			}
			
			// insert_data ? HLS			
			sql = "SELECT * FROM content_hls_enc WHERE encd_piqu_typ_cd IN ( 0, 2 ) and status='[원본]작업대기' AND (extra_status & ?)=0  ORDER BY insert_time ASC";
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
			
			if (itemsERResv.isEmpty() == false) {
				items.addAll(itemsERResv);				
			}
			
			if (itemsER.isEmpty() == false) {
				items.addAll(itemsER);				
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

}