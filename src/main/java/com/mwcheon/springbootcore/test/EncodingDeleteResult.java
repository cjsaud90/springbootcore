package com.mwcheon.springbootcore.test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import common.DbCommonHelper;
import common.DbHelper;
import common.DbTransaction;
import common.EmsException;
import common.JsonValue;
import common.ProfileAssignInfo;
import common.ServletHelper;
import common.Util;
import common.ncms.ReportNCMS;
import common.ncms.ReportNCMSHls;
import common.system.LoadBalancerEncoderElemental;
import common.system.SystemItem;
import controller.BroadcastTaskManager;
import controller.TranscodingStatus;
import io.EMSProcServiceCopyFileIo;
import io.EMSProcServiceDeleteFileIo;
import task.pipeline.EncodingPipeline;

/**
 * Servlet implementation class EncodingDeleteResult
 */
@WebServlet("/api/encdelete")
public class EncodingDeleteResult extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static Logger logger = LoggerFactory.getLogger(EncodingDeleteResult.class);
	private static Object m_lcok = new Object();
	
    /**
     * @see HttpServlet#HttpServlet()
     */
    public EncodingDeleteResult() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
//	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
//		JSONObject jsonResponseBody = new JSONObject();
//
//		try {
//			String content = ServletHelper.getBody(request);
//			logger.info("content: " + content);
//			JSONObject json = (JSONObject) JSONValue.parse(content);
//			
//			jsonResponseBody.put("error", 0);
//			jsonResponseBody.put("desc", "success");
//			
//			new Thread(new Runnable() {
//				@Override
//				public void run() {
//					try {
//						String cid = JsonValue.getString(json, "cid");
//						String sid = JsonValue.getString(json, "sid");
//						String server_ver = JsonValue.getString(json, "server_ver");
//						long error = (long) JsonValue.get(json, "error");
//						String desc = JsonValue.getString(json, "desc");
//						
//						if (error == 0) {
//							DbTransaction.updateDbSql("UPDATE content SET status='[원본]인코딩취소', last_time=now(3), worker='EAMS', note='" + desc +"'  WHERE cid='" + cid + "'");
//							//인코딩 취소 시 엘리멘탈 in/out에 남아있는 소재파일 삭제 진행
//							//2017.06.28 김명종M
//							String calling_task_storage_ip = null;
//							DbTransaction db = new DbTransaction();
//							Connection conn = null;							
//							try{		
//								conn = db.startAutoCommit();
//								try (PreparedStatement pstmt = conn.prepareStatement("SELECT cems.configure.value FROM cems.configure WHERE cems.configure.key='content_task_storage_ip'")) {																		
//									try( ResultSet rs = pstmt.executeQuery() ){
//										if( rs.next() ){
//											calling_task_storage_ip = rs.getString(1);
//										}
//									}
//								}								
//								
//								SystemItem system = ElementalEncoderLoadBalancer.getSystemTbl().get(Long.valueOf(sid));
//								if(system != null && calling_task_storage_ip != null && !calling_task_storage_ip.isEmpty() ){
//									removeEncInOutFile(cid, system, calling_task_storage_ip);									
//								}else{
//									logger.info( String.format( "system tbl info get failure | [ cid : %s ][ sid : %s ]", cid, sid ) );
//								}
//							}catch( Exception ex ){
//								logger.error("", ex);
//							}finally{
//								db.close();
//							}
//							
//						} else {
//							DbTransaction.updateDbSql("UPDATE content SET status='[원본]인코딩취소실패', last_time=now(3), worker='EAMS', note='" + desc + "'  WHERE cid='" + cid + "'");
//						}
//						
//						DbHelper.insertContentHistroy("cid='" + cid + "'");
//						
//						TranscodingStatus.signal();
//						BroadcastTaskManager.signal();
//						
//					} catch (Exception e) {
//						logger.error(e.getMessage());
//					}
//				}
//			}).start();
//
//		} catch (Exception e) {
//			logger.error("", e);
//			jsonResponseBody.put("error", EmsException.code(e));
//			jsonResponseBody.put("desc", EmsException.msg(e));
//		}
//
//		response.setContentType("application/json");
//		response.setCharacterEncoding("utf-8");
//		logger.info(jsonResponseBody.toString());
//		response.getWriter().write(jsonResponseBody.toString());
//	}
    
    
//    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
//		JSONObject jsonResponseBody = new JSONObject();
//
//		try {
//			String content = ServletHelper.getBody(request);
//			logger.info("content: " + content);
//			JSONObject json = (JSONObject) JSONValue.parse(content);
//			
//			jsonResponseBody.put("error", 0);
//			jsonResponseBody.put("desc", "success");
//			
//			new Thread(new Runnable() {
//				@Override
//				public void run() {
//					try {
//						ArrayList<String> listCids = new ArrayList<>();
//						
//						String strCid = JsonValue.getString(json, "cid", true );
//						if( strCid != null && !strCid.isEmpty() ) {
//							listCids.add( strCid );
//						}
//						String sid = JsonValue.getString(json, "sid", true );
//						String server_ver = JsonValue.getString(json, "server_ver", true );
//						long error = (long) JsonValue.get(json, "error");
//						String desc = JsonValue.getString(json, "desc", true );
//						
//						String strIsDummy = JsonValue.getString(json, "is_dummy", true );
//						
//						try{
//							if( json.get("cids_dummy") != null ){
//								JSONArray jsonArrCidsDummy = (JSONArray) json.get("cids_dummy");			
//								if( jsonArrCidsDummy != null ) {
//									for(int i = 0 ; i < jsonArrCidsDummy.size(); i++) {
//										
//										String strCidDummy = (String) jsonArrCidsDummy.get(i);
//										if( strCidDummy != null && !strCidDummy.isEmpty() ) {
//											listCids.add( strCidDummy );
//										}
//									}
//								}
//							}				
//						}catch( Exception ex3 ){}
//						
//						for( String strCidPrint : listCids ) {
//							logger.info( String.format("Enc Cancel CID : %s", strCidPrint ) );
//						}
//						
//						for( String cid : listCids ) {
//							boolean bRtsp = true;
//							if( !Util.IdTypeIsRTSP( cid ) ) {
//								bRtsp = false;
//							}
//							
//							
//							if (error == 0) {
//								if( bRtsp ) {
//									DbTransaction.updateDbSql("UPDATE content SET status='[원본]인코딩취소', last_time=now(3), worker='EAMS', note='" + desc +"'  WHERE cid='" + cid + "'");	
//								}else {
//									DbTransaction.updateDbSql("UPDATE content_hls_enc SET status='[원본]인코딩취소', last_time=now(3), worker='EAMS', note='" + desc +"'  WHERE cid='" + cid + "'");
//								}
//								
//								//인코딩 취소 시 엘리멘탈 in/out에 남아있는 소재파일 삭제 진행
//								//2017.06.28 김명종M
//								String calling_task_storage_ip = null;
//								DbTransaction db = new DbTransaction();
//								Connection conn = null;							
//								try{		
//									conn = db.startAutoCommit();
//									try (PreparedStatement pstmt = conn.prepareStatement("SELECT cems.configure.value FROM cems.configure WHERE cems.configure.key='content_task_storage_ip'")) {																		
//										try( ResultSet rs = pstmt.executeQuery() ){
//											if( rs.next() ){
//												calling_task_storage_ip = rs.getString(1);
//											}
//										}
//									}								
//									
//									SystemItem system = ElementalEncoderLoadBalancer.getSystemTbl().get(Long.valueOf(sid));
//									if(system != null && calling_task_storage_ip != null && !calling_task_storage_ip.isEmpty() ){
//										removeEncInOutFile(cid, system, calling_task_storage_ip);									
//									}else{
//										logger.info( String.format( "system tbl info get failure | [ cid : %s ][ sid : %s ]", cid, sid ) );
//									}
//								}catch( Exception ex ){
//									logger.error("", ex);
//								}finally{
//									db.close();
//								}
//								
//							} else {
//								if( bRtsp ) {
//									DbTransaction.updateDbSql("UPDATE content SET status='[원본]인코딩취소실패', last_time=now(3), worker='EAMS', note='" + desc + "'  WHERE cid='" + cid + "'");	
//								}else {
//									DbTransaction.updateDbSql("UPDATE content_hls_enc SET status='[원본]인코딩취소실패', last_time=now(3), worker='EAMS', note='" + desc + "'  WHERE cid='" + cid + "'");
//								}
//								
//							}
//							
//							if( bRtsp ) {
//								DbHelper.insertContentHistroy("cid='" + cid + "'");
//							}	
//						}
//						
//						TranscodingStatus.signal();
//						BroadcastTaskManager.signal();
//						
//					} catch (Exception e) {
//						logger.error(e.getMessage());
//					}
//				}
//			}).start();
//
//		} catch (Exception e) {
//			logger.error("", e);
//			jsonResponseBody.put("error", EmsException.code(e));
//			jsonResponseBody.put("desc", EmsException.msg(e));
//		}
//
//		response.setContentType("application/json");
//		response.setCharacterEncoding("utf-8");
//		logger.info(jsonResponseBody.toString());
//		response.getWriter().write(jsonResponseBody.toString());
//	}
    
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		JSONObject jsonResponseBody = new JSONObject();

		try {
			String content = ServletHelper.getBody(request);
			logger.info("content: " + content);
			JSONObject json = (JSONObject) JSONValue.parse(content);
			
			jsonResponseBody.put("error", 0);
			jsonResponseBody.put("desc", "success");
			
			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						ArrayList<String> listCids = new ArrayList<>();
						ArrayList<Object[]> listRemoveEncFileInfo = new ArrayList<>();
						String strConStorageIp = new DbCommonHelper().GetConfigureDBValue(null, "content_task_storage_ip");
						
						String strCid = JsonValue.getString(json, "cid", true );
						if( strCid != null && !strCid.isEmpty() ) {
							listCids.add( strCid );
						}
						String sid = JsonValue.getString(json, "sid", true );
						String server_ver = JsonValue.getString(json, "server_ver", true );
						long error = (long) JsonValue.get(json, "error");
						String desc = JsonValue.getString(json, "desc", true );
						
						String strIsDummy = JsonValue.getString(json, "is_dummy", true );
						
						try{
							if( json.get("cids_dummy") != null ){
								JSONArray jsonArrCidsDummy = (JSONArray) json.get("cids_dummy");			
								if( jsonArrCidsDummy != null ) {
									for(int i = 0 ; i < jsonArrCidsDummy.size(); i++) {
										
										String strCidDummy = (String) jsonArrCidsDummy.get(i);
										if( strCidDummy != null && !strCidDummy.isEmpty() ) {
											listCids.add( strCidDummy );
										}
									}
								}
							}				
						}catch( Exception ex3 ){}
						
						for( String strCidPrint : listCids ) {
							logger.info( String.format("Enc Cancel CID : %s", strCidPrint ) );
						}						
								
						//인코딩 취소 시 엘리멘탈 in/out에 남아있는 소재파일 삭제 진행
						//2017.06.28 김명종M
						synchronized ( m_lcok ) {
							
							for( String cid : listCids ) {
																
								DbTransaction db = new DbTransaction();
								Connection conn = null;							
								try{											
									conn = db.start();
									
									boolean bRtsp = true;
									long lSid = -1;
									long lProfileId = -1;
									String sql;
									String strMediaId = "";
									
									if( !Util.IdTypeIsRTSP( cid ) ) {
										bRtsp = false;
									}
									
									if( bRtsp ) {
										sql = "select * from cems.content where cid=?";
									}else{
										sql = "select * from cems.content_hls_enc where cid=?";
									}
									
									try (PreparedStatement pstmt = conn.prepareStatement(sql)) {		
										pstmt.setString(1, cid );
										try( ResultSet rs = pstmt.executeQuery() ){
											if( rs.next() ){
												lSid = rs.getLong("sid");
												if( rs.wasNull() ) { lSid = -1; }
												lProfileId = rs.getLong("profile_id");
												if( rs.wasNull() ) { lProfileId = -1; }
												if( !bRtsp ) {
													strMediaId = rs.getString("mda_id");
												}											
											}
										}
									}
									
									if( lSid > -1 && lProfileId > -1 ) {
										
										ProfileAssignInfo.ProfileInfo pInfo = ProfileAssignInfo.getProfileType(conn, lProfileId);
//										int assign = ProfileAssignInfo.getAssign(conn, pInfo.type,pInfo.resolution,pInfo.strDivision);
										int assign = ProfileAssignInfo.getEncoderAssign(conn, pInfo.type,pInfo.resolution,pInfo.strDivision, lSid);
										if (assign != -1) {
											
											int nEncCopyTaskCnt = EncodingPipeline.IsEncCopyOutFileTaskCnt( conn, cid );
											if( nEncCopyTaskCnt > 0 ) {
												assign = ( assign * ( nEncCopyTaskCnt + 1 ) );
											}
											
											LoadBalancerEncoderElemental.returnIdle(conn, lSid, assign );
											int nAssignCur = LoadBalancerEncoderElemental.GetAssignCur( conn, lSid );
											logger.info("=======================================================================");
											logger.info(String.format("Enc Assign Return OK | [ CID : %s ] [ EncCopyTaskCnt : %d ] [ Type : %s ] [ Framesize : %s ] [ SID : %d ] [ Return Assign : %d ] [ Cur Assign : %d ]",
													cid,
													nEncCopyTaskCnt,
													pInfo.type,
													pInfo.resolution,
													lSid, 
													assign, 
													nAssignCur 
													));
											logger.info("=======================================================================");
										}
										
									}
									
									if (error == 0) {
										if( bRtsp ) {
											DbTransaction.updateDbSql( conn, "UPDATE content SET status='[원본]인코딩취소', last_time=now(3), worker='EAMS', note='" + desc +"'  WHERE cid='" + cid + "'");
											DbHelper.insertContentHistroy( conn, "cid='" + cid + "'");
										}else {
											DbTransaction.updateDbSql( conn, "UPDATE content_hls_enc SET status='[원본]인코딩취소', last_time=now(3), worker='EAMS', note='" + desc +"'  WHERE cid='" + cid + "'");
										}
									}else {
										if( bRtsp ) {
											DbTransaction.updateDbSql( conn, "UPDATE content SET status='[원본]인코딩취소실패', last_time=now(3), worker='EAMS', note='" + desc + "'  WHERE cid='" + cid + "'");
											DbHelper.insertContentHistroy(conn, "cid='" + cid + "'");
										}else {
											DbTransaction.updateDbSql( conn,"UPDATE content_hls_enc SET status='[원본]인코딩취소실패', last_time=now(3), worker='EAMS', note='" + desc + "'  WHERE cid='" + cid + "'");
										}
									}
									
									if( bRtsp ) {
										new ReportNCMS().ReportEpsdResolutionInfo( cid, ReportNCMS.STATUS_CODE.ErrorTask );
									}else {
										if( strMediaId != null && !strMediaId.isEmpty() ) {
											boolean bLastError = false;

											if( DbCommonHelper.IsHlsLastErrorCheckItem( conn, strMediaId, cid ) ) {
												bLastError = true;	
											}								
													
											if( bLastError ) {
												DbTransaction.updateDbSql( conn, String.format( "UPDATE content_hls SET mda_matl_sts_cd='작업오류' WHERE mda_id='%s'", strMediaId ) );
												DbTransaction.updateDbSql( conn, String.format("UPDATE content set content_status='작업오류', status='작업오류' WHERE cid='%s'", strMediaId ));
												new ReportNCMSHls().mediaEncodingResult( conn, strMediaId, ReportNCMS.STATUS_CODE.ErrorTask );	
											}else {
												logger.info( String.format( String.format("Is Not Last Error Hls Sche, Skip Task Error Matl Sts Cd Setting | [ %s ]", strMediaId ) ) );
											}	
										}
									}
									
									Object[] objs = new Object[2];
									objs[0] = cid;
									objs[1] = lSid;								
									listRemoveEncFileInfo.add(objs);
									
									db.commit();
								}catch( Exception ex ){
									db.rollback();
									logger.error("", ex);
								}finally{
									db.close();
									conn = null;
								}
							}
							
						}
						
						for( Object[] objs : listRemoveEncFileInfo ) {
							String strRemoveEncFileCid = (String)objs[0];
							Long lRemoveEncFileSid = (Long)objs[1];
							SystemItem system = null;
							
							if( lRemoveEncFileSid != null && lRemoveEncFileSid.longValue() > -1 ) {
								system = LoadBalancerEncoderElemental.getSystemTbl().get( lRemoveEncFileSid.longValue() );
							}							
							
							if(system != null && strConStorageIp != null && !strConStorageIp.isEmpty() ){
								removeEncInOutFile( null, strRemoveEncFileCid, system, strConStorageIp);									
							}else{
								logger.info( String.format( "system tbl info get failure | [ cid : %s ][ sid : %s ]", strRemoveEncFileCid, lRemoveEncFileSid.longValue() ) );
							}
						}
																	
						TranscodingStatus.signal();
						BroadcastTaskManager.signal();
					
					} catch (Exception e) {
						logger.error(e.getMessage());
					}
				
				}
			}).start();

		} catch (Exception e) {
			logger.error("", e);
			jsonResponseBody.put("error", EmsException.code(e));
			jsonResponseBody.put("desc", EmsException.msg(e));
		}

		response.setContentType("application/json");
		response.setCharacterEncoding("utf-8");
		logger.info(jsonResponseBody.toString());
		response.getWriter().write(jsonResponseBody.toString());
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}
	
	protected static synchronized Object[] ChkEndInOutFile( Connection paramConn, String cid, SystemItem system ) throws Exception
	{
		Object[] objRet = new Object[3];
		
		DbTransaction db = null;
		Connection conn = null;
		
		boolean remove = false;
		boolean bRtsp = true;
		String _1st_filepath = "";
		String smi_cvt_filepath = "";
		String sql = null;
		
		try {		
			if( paramConn == null ) {
				db = new DbTransaction();
				conn = db.startAutoCommit();
			}else {				
				conn = paramConn;
			}
			
			if( !Util.IdTypeIsRTSP(cid) ) {
				bRtsp = false;
			}
			
			if( bRtsp ) {
				sql = "SELECT epsd_id, possn_yn, set_num, type, 1st_filepath, sub_cvt_filepath FROM content WHERE cid=?";
			}else {
				sql = "SELECT mda_id, set_num, type, 1st_filepath, sub_cvt_filepath FROM content_hls_enc WHERE cid=?";
			}			
			
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setString(1, cid);
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {
						String strEpsdId = "";
						String strPossnYn = "";
						String strMediaID = "";
						if( bRtsp ) {
							strEpsdId = rs.getString("epsd_id");
							strPossnYn = rs.getString("possn_yn");
						}else {
							strMediaID = rs.getString( "mda_id" );									
						}
						String setNum = rs.getString("set_num");
						String type = rs.getString("type");
						_1st_filepath = Util.ConvNullIsEmptyStr( rs.getString("1st_filepath") );
						smi_cvt_filepath = Util.ConvNullIsEmptyStr( rs.getString("sub_cvt_filepath") );
						
						if (setNum != null && setNum.isEmpty() == false) { // SET 인 경우, 인코딩 중인 항목이 없으면 원본 파일 삭제							
							int nCntRtsp = 0;
							int nCntHLS = 0;
							
							// RTSP
							try (PreparedStatement pstmt2 = conn.prepareStatement("SELECT COUNT(*) FROM content WHERE sid=? and set_num=? AND content_status='작업진행' AND status like '%%인코딩%%' and status not like '%%실패%%' and status not like '%%취소%%' and status not like '%%오류%%'")) {
								int args = 1;
								pstmt2.setLong(args++, system.id);
								pstmt2.setString(args++, setNum);
								try( ResultSet rs2 = pstmt2.executeQuery() ){
									if (rs2.next()) {
										nCntRtsp = rs2.getInt(1);
										logger.info( String.format( "remained SET RTSP '작업진행'-'인코딩' | [ Remained Cnt : %d ] [ SID : %d ] [ Enc Name : %s ]", nCntRtsp, system.id, system.name ) ); 
										
									}
								}
							}
							
							// HLS
							try (PreparedStatement pstmt2 = conn.prepareStatement("SELECT COUNT(*) FROM content_hls_enc WHERE  sid=? and set_num=? AND status like '%%인코딩%%' and status not like '%%실패%%' and status not like '%%취소%%' and status not like '%%오류%%'")) {
								int args = 1;
								pstmt2.setLong(args++, system.id);
								pstmt2.setString(args++, setNum);
								try( ResultSet rs2 = pstmt2.executeQuery() ){
									if (rs2.next()) {
										nCntHLS = rs2.getInt(1);
										logger.info( String.format( "remained SET HLS '작업진행'-'인코딩' | [ Remained Cnt : %d ] [ SID : %d ] [ Enc Name : %s ]", nCntHLS, system.id, system.name ) );
										
									}
								}
							}
							
							if (nCntRtsp == 0 && nCntHLS == 0 ) { // 인코딩 상태의 작업진행 중인 SET 항목이 없으면 원본 삭제
								remove = true;
							}							
							
						} else { // 모바일 인 경우
							
							if( bRtsp ) {
								
								if (type.toUpperCase().equals("MOBILE") ){
									sql = "SELECT COUNT(*) FROM content WHERE epsd_id=? and possn_yn=? AND UPPER(type)='MOBILE' AND content_status='작업진행' AND status like '%%인코딩%%' and status not like '%%실패%%' and status not like '%%취소%%' and status not like '%%오류%%'";
									try (PreparedStatement pstmt2 = conn.prepareStatement( sql )) {
										int args = 1;
										pstmt2.setString(args++, strEpsdId );
										pstmt2.setString(args++, strPossnYn );
										try( ResultSet rs2 = pstmt2.executeQuery() ){
											if (rs2.next()) {
												int cnt = rs2.getInt(1);
												logger.info( String.format( "remained RTSP MOBILE '작업진행'-'인코딩' | [ Remained Cnt : %d ] [ SID : %d ] [ Enc Name : %s ]", cnt, system.id, system.name ) );
												if (cnt == 0) { // 인코딩 상태의 작업진행 중인 모바일 항목이 // 없으면 원본 삭제
													remove = true;
												}
											}
										}
									}		
								}else {									
									remove = true;									
								}
								
							}else {
								sql = "SELECT COUNT(*) FROM content_hls_enc WHERE mda_id=? and sid=? AND status like '%%인코딩%%' and status not like '%%실패%%' and status not like '%%취소%%' and status not like '%%오류%%'";
								try (PreparedStatement pstmt2 = conn.prepareStatement( sql )) {
									int args = 1;
									pstmt2.setString(args++, strMediaID );
									pstmt2.setLong(args++, system.id );
									try( ResultSet rs2 = pstmt2.executeQuery() ){
										if (rs2.next()) {
											int cnt = rs2.getInt(1);
											logger.info( String.format( "remained HLS '작업진행'-'인코딩' | [ Remained Cnt : %d ] [ SID : %d ] [ Enc Name : %s ]", cnt, system.id, system.name ) );
											if (cnt == 0) { // 인코딩 상태의 작업진행 중인 모바일 항목이 // 없으면 원본 삭제
												remove = true;
											}
										}
									}
								}
							}
							
						}
					}
				}
			} finally {
				if( paramConn == null ) {
					db.close();
				}				
			}	
			
		}catch( Exception ex ) {
			logger.error("",ex);
			throw ex;
		}finally {
			objRet[0] = remove;
			objRet[1] = _1st_filepath;
			objRet[2] = smi_cvt_filepath;
		}
		
		return objRet;
	}
	
	protected static void removeEncInOutFile( Connection conn, String cid, SystemItem system, String calleeIp) throws Exception {
		
		Object[] objRet = ChkEndInOutFile( conn, cid, system );
		if( objRet == null || objRet.length < 3 ) {
			logger.error( String.format("ChkEndInOutFile Result is UnNormal | [ CID : %s ] [ system id : %d ]", cid, system.id ) );
			return;
		}
		
		boolean remove = (boolean)objRet[0];
		String _1st_filepath = (String)objRet[1];
		String smi_cvt_filepath = (String)objRet[2];
				
		if ( remove && _1st_filepath != null && !_1st_filepath.isEmpty()) {
			// Input File Remove			
			Path encOrigin = Paths.get(system.originPath);
			String filename = Paths.get(_1st_filepath).getFileName().toString();
			String delpath = "\\\\" + system.ip + encOrigin.resolve(filename).toString().replace('/', '\\');
			EMSProcServiceDeleteFileIo delIo = new EMSProcServiceDeleteFileIo(delpath, null);
			delIo.setSrcIp(calleeIp);
			delIo.start(null);
			
			delpath = "\\\\" + system.ip + encOrigin.resolve(filename + ".mov").toString().replace('/', '\\');
			logger.info( String.format("Wartermark Dummy Mov File Delete | [ %s ]", delpath) );
			delIo = null;
			delIo = new EMSProcServiceDeleteFileIo(delpath, null);
			delIo.setSrcIp(calleeIp);
			delIo.start(null);
			
			delpath = "\\\\" + system.ip + encOrigin.resolve(filename + ".mov.W0.ts").toString().replace('/', '\\');
			logger.info( String.format("Wartermark Dummy W0 File Delete | [ %s ]", delpath) );
			delIo = null;
			delIo = new EMSProcServiceDeleteFileIo(delpath, null);
			delIo.setSrcIp(calleeIp);
			delIo.start(null);
						
			delpath = "\\\\" + system.ip + encOrigin.resolve(filename + ".mov.W1.ts").toString().replace('/', '\\');
			logger.info( String.format("Wartermark Dummy W1 File Delete | [ %s ]", delpath) );
			delIo = null;
			delIo = new EMSProcServiceDeleteFileIo(delpath, null);
			delIo.setSrcIp(calleeIp);
			delIo.start(null);
			
			if( smi_cvt_filepath != null && !smi_cvt_filepath.isEmpty() ){
				//SMI Remove
				filename = Paths.get(smi_cvt_filepath).getFileName().toString();
				delpath = "\\\\" + system.ip + encOrigin.resolve(filename).toString().replace('/', '\\');
				delIo = null;
				delIo = new EMSProcServiceDeleteFileIo(delpath, null);
				delIo.setSrcIp(calleeIp);
				delIo.start(null);
			}			
		}
		
		if( _1st_filepath != null && !_1st_filepath.isEmpty() ){
			//OutPut File Remove
			//Output File은 각 CID 별로 존재함으로 그냥 삭제진행한다.
			String convCid = cid.replace("{", "").replace("}", "").replace("[", "_").replace("]", "");
			String fileName = Paths.get( _1st_filepath ).getFileName().toString();
			String pureFileName = fileName.substring( 0, fileName.lastIndexOf('.') );
			String outputFileName = pureFileName + convCid + ".ts";
			String elementalOutputPath = "\\\\" + system.ip + Paths.get(system.outputPath).resolve( outputFileName ).toString().replace("/", "\\");
						
			EMSProcServiceDeleteFileIo delIo = new EMSProcServiceDeleteFileIo(elementalOutputPath, null);			
			delIo.setSrcIp(calleeIp);
			delIo.start(null);
		}		
	}

}
