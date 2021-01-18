package com.mwcheon.springbootcore.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import common.system.LoadBalancerEncoderBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import common.DbTransaction;
import common.EmsException;
import common.ExtraStatusManipulation;
import common.ProfileAssignInfo;
import common.WhoAmI;
import common.system.GalaxiaEncoderLoadBalancer;
import common.system.LoadBalancerEncoderElemental;
import controller.ClipTaskManager;
import task.TaskProcessorManager;
import task.pipeline.ClipElementalEncodingPipeline;
import task.pipeline.ClipEncodingPipeline;
import task.pipeline.Pipeline;

public class DispatcherEncodingTaskElementalClip extends DispatcherEncodingTaskBase  {
	private static Logger logger = LoggerFactory.getLogger(DispatcherEncodingTaskElementalClip.class);	

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
				ArrayList<ReadyItem> items = getReadyItem("clip");
				if (items.isEmpty()) {
					sleep(2000);
					continue;
				}
				
				ReadyItem encItem = null;
				int totalAssign = 0;
				for (ReadyItem item : items) {
					Map<String, Object> encoderAssign = new HashMap<>();
					encoderAssign = getAssign("clip", item.cid, item.sid);
//					totalAssign = getAssign("clip", item.cid);
					if( totalAssign == 0 ){
						continue;
					}
					totalAssign = (int) encoderAssign.get("assign");
					
					if (item.sid != null) {
						// 작업예약된 장비가 가용한지 확인
						if (LoadBalancerEncoderElemental.availableClip(item.sid, item.hasUhd, totalAssign, true )) {
							// 가용하면 인코딩 진행
							encItem = item;
							LoadBalancerEncoderElemental.getSystem(item.sid, totalAssign);
							break;		
						}
					} else {
						// 가용한 장비가 있는지 확인
//						Long sid = LoadBalancerEncoderElemental.availableClip( item.hasUhd, totalAssign, true );
						Long sid  = (Long) encoderAssign.get("sid");
						if (sid != -1) {
							LoadBalancerEncoderElemental.getSystem(sid, totalAssign);
							// 가용하면 인코딩 진행
							item.sid = sid;
							item.encType = enc_type.Elemenatal;
							encItem = item;
							break;							
						}else{
							// 가용한 장비가 있는지 확인
							sid = GalaxiaEncoderLoadBalancer.available(1, false, "clip");
							if (sid == -1) {
								continue;
							}

							GalaxiaEncoderLoadBalancer.getSystem(sid, 1);
							// 가용하면 인코딩 진행
							item.sid = sid;
							item.encType = enc_type.Galaxia;
							encItem = item;
							break;
						}
												
					}
				}
				
				if (encItem == null) {
					sleep(1000); // 사용자가 편성항목에 대해 상태를 변경할 수 있기에 잠시 쉬었다 다시 조회한다. (ex HOLD)
					continue;
				}
				
				logger.info("new dispatchable item [" + encItem.cid + "][" + encItem.type + "]");
				
				setWorkingStatus("clip", encItem.cid, encItem.sid);
				
				// create working pipeline 
				Pipeline pipeline = null;
				if( encItem.encType == enc_type.Elemenatal ){
					pipeline = new ClipElementalEncodingPipeline();
				}else if( encItem.encType == enc_type.Galaxia ){
					pipeline = new ClipEncodingPipeline();
				}else{
					logger.error(String.format("Unknown Enc dispatcher type | [ cid : %s] [ sid : %d ] [ enc type : %s ] ", encItem.cid, encItem.sid, encItem.encType.toString() ));
					continue;  
				}
				
				logger.info("execute [" + encItem.cid + "] -> task-processor[" + pipeline + "]");
								
				// launch task
				TaskProcessorManager.run(encItem.cid, pipeline, totalAssign );
			} catch (InterruptedException e) {
				break;
			} catch (Exception e) {
				logger.error("", e);
			}
		}		
	}
	
	protected ArrayList<ReadyItem> getReadyItem(String tbl) {
		ArrayList<ReadyItem> items = new ArrayList<>();
		ArrayList<ReadyItem> itemsNorm = new ArrayList<>();
		ArrayList<ReadyItem> itemsResv = new ArrayList<>(); // 작업예약
		
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		try {
			conn = db.startAutoCommit();
			String sql = null;
			
			// 편성일 기준
			sql = "SELECT * FROM " + tbl + " WHERE status='[원본]작업대기' AND (extra_status & ?)=0 ORDER BY schedule_time ASC";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setInt(1, ExtraStatusManipulation.Flag.EF_HOLD_ON.get());
				try( ResultSet rs = pstmt.executeQuery() ){
					while (rs.next()) {
						ReadyItem item = new ReadyItem();
						item.cid = rs.getString("cid");
						item.frameSize = rs.getString("framesize");
						if (item.hasUhd == false && rs.getString("framesize").toUpperCase().equals("UHD")) {
							item.hasUhd = true;
						}
						int extraStatus = rs.getInt("extra_status");
						if ((extraStatus & ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get()) != 0) {
							item.sid = rs.getLong("sid");
							//logger.info("found reserved job! cid=" + item.cid + ", sid=" + item.sid);
							itemsResv.add(item);
						} else {
							itemsNorm.add(item);
						}
					}
				}
			}
			
			if (itemsResv.isEmpty() == false) {
				items.addAll(itemsResv);				
			}		
			items.addAll(itemsNorm);
			return items;
			
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			db.close();
		}
		return null;
	}	
	
	protected int getAssign(String tbl, String cid) throws Exception 
	{
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		int assign = 0;		

		try {
			conn = db.startAutoCommit();			
			String sql = null;			
			sql = "SELECT * FROM " + tbl + " WHERE cid=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setString(1, cid);
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {									
						Integer pid = (Integer)rs.getObject("profile_id");
						if( pid != null ){
							ProfileAssignInfo.ProfileInfo proInfo = ProfileAssignInfo.getProfileType(conn, pid);
							int temp = ProfileAssignInfo.getAssign(conn, proInfo.type, proInfo.resolution, proInfo.strDivision );
							if( temp != -1 ){
								assign += temp; 
							}else{
								throw new EmsException( String.format( "Assign getting is fail | Assign : %d | [ tbl : %s ] [ cid : %s ] [ pid : %d ]" , temp, tbl,  cid, pid) );
							}
							
						}else{
							logger.error(String.format( "Profile is is null | [ tbl : %s ] [ cid : %s ]" , tbl,  cid) );
							return 0;
						}											
					}
				}
			}
				
		} finally {
			db.close();
		}
		
		return assign;
	}

	protected Map<String, Object> getAssign(String tbl, String cid, Long lSid) throws Exception
	{
		Map<String, Object> resultAssign = new HashMap<>();
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		int assign = 0;
		Map<Long, Map<String, Integer>> encoderAssign = new HashMap<>();

		try {
			conn = db.startAutoCommit();
			String sql = null;
			String selSysSql = null;

			sql = "SELECT * FROM " + tbl + " WHERE cid=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setString(1, cid);
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {
						Integer pid = (Integer)rs.getObject("profile_id");
						if( pid != null ){
							ProfileAssignInfo.ProfileInfo proInfo = ProfileAssignInfo.getProfileType(conn, pid);

							String type = proInfo.type;
							String resolution = proInfo.resolution;
							String strDivision = proInfo.strDivision;
							String targetColumn = LoadBalancerEncoderBase.getColumn(type, resolution);

							if(lSid != -1){
								selSysSql = "SELECT * FROM cems.system WHERE id = '" + lSid + "'";
							}else{
								selSysSql = "SSELECT * FROM cems.system WHERE cems.system.type='Encoder' AND vendor="+strDivision;
							}


							try (PreparedStatement pstmt2 = conn.prepareStatement(selSysSql)) {
								try (ResultSet rs2 = pstmt2.executeQuery()) {
									System.out.println(String.format("get system sql [%s]", selSysSql));
									while (rs2.next()) {

										long id = rs2.getInt("id"); // 수동으로 설정한 최대설정값
										int encMaxAssign = rs2.getInt("enc_max_assign"); // 수동으로 설정한 최대설정값
										int assigned = rs2.getInt("assign"); // 이미 할당된 점수
										int targetAssign = rs2.getInt(targetColumn); // 할당에 필요한 점수
										if(encoderAssign.get(id) == null || encoderAssign.size() == 0){

											Map<String, Integer> addItem = new HashMap<>();
											addItem.put("assign", targetAssign);
											addItem.put("encMaxAssign", encMaxAssign);
											addItem.put("assigned", assigned);

											encoderAssign.put(id, addItem);
										}else{
											Map<String, Integer> item = encoderAssign.get(id);
											Integer saveAssign = item.get("assign") + targetAssign;
											item.put("assign", saveAssign);
										}
									}
								}
							}


							encoderAssign.forEach((key, value)->{
								int totalAssigned = value.get("assign") + value.get("assigned");
								int encMaxAssign = value.get("encMaxAssign");
								int diffValue = encMaxAssign - totalAssigned;

								if(resultAssign.isEmpty()){
									resultAssign.put("sid", key);
									resultAssign.put("assign", value.get("assign"));
									resultAssign.put("diffValue",  diffValue);
								}else{
									int getDiffValue = (int) resultAssign.get("diffValue");
									if(diffValue >  getDiffValue){
										resultAssign.put("sid", key);
										resultAssign.put("assign", value.get("assign"));
										resultAssign.put("diffValue", diffValue);
									}
								}
							});

						}else{
							logger.error(String.format( "Profile is is null | [ tbl : %s ] [ cid : %s ]" , tbl,  cid) );
							return null;
						}
					}
				}
			}

		} finally {
			db.close();
		}

		return resultAssign;
	}
	
	protected void setWorkingStatus(String tbl, String cid, long sid) throws Exception {
		ArrayList<String> cids = new ArrayList<>();

		DbTransaction db = new DbTransaction();
		Connection conn = null;

		try {
			conn = db.start();
			try (PreparedStatement pstmt = conn.prepareStatement("UPDATE " + tbl + " SET status='[원본]작업준비', sid=? WHERE cid=?")) {
				pstmt.setLong(1, sid);
				pstmt.setString(2, cid);
				logger.debug(pstmt.toString());
				pstmt.executeUpdate();
			}
			
			db.commit();
			
			ClipTaskManager.signal();;
			
		} catch (Exception e) {
			db.rollback();
			throw e;
		} finally {
			db.close();
		}
	}
}