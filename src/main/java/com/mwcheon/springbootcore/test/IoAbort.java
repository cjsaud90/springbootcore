package com.mwcheon.springbootcore.test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import common.Checker;
import common.DbCommonHelper;
import common.DbHelper;
import common.DbTransaction;
import common.Define;
import common.EmsException;
import common.ErrorCode;
import common.ProfileAssignInfo;
import common.ServletHelper;
import common.Util;
import common.ncms.ReportNCMS;
import common.ncms.ReportNCMSHls;
import common.pertitle.RequestPertitleVmafRemove;
import common.pertitle.RequestPertitleVmafState;
import common.pertitle.RequestPertitleVmafStateJob;
import common.pertitle.RequestPertitleVmafStop;
import common.system.LoadBalancerEncoderElemental;
import common.system.GalaxiaEncoderLoadBalancer;
import common.system.MspsLoadBalancer;
import io.EMSProcServiceDeleteFileIoEx;
import io.FileIoHandler;
import task.pipeline.EncodingPipeline;

/**
 * Servlet implementation class EncodingAbort
 */
@WebServlet("/IoAbort")
public class IoAbort extends HttpServlet implements FileIoHandler {
	private static final long serialVersionUID = 1L;
	private static Logger logger = LoggerFactory.getLogger(IoAbort.class);

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public IoAbort() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		logger.info(ServletHelper.getFullURL(request));
		request.setCharacterEncoding("UTF-8");
		
		String q = request.getParameter("q");
		if (q == null || q.isEmpty()) {
			ioAbortContent(request, response);
		} else {
			ioAbortClip(request, response);
		}		
	}
	
	private void ioAbortClip(HttpServletRequest request, HttpServletResponse response) throws IOException {
		JSONObject jsonResponse = new JSONObject();

		DbTransaction db = new DbTransaction();
		Connection conn = null;

		// 변경할 CID
		String cid = request.getParameter("cid");
		String worker = request.getParameter("worker");
		String note = request.getParameter("note");

		try {
			conn = db.start();

			try (PreparedStatement pstmt1 = conn.prepareStatement("SELECT * FROM clip WHERE cid=?")) {
				pstmt1.setString(1, cid);
				try( ResultSet rs = pstmt1.executeQuery() ){
					rs.next();
					
					String currentStatus = rs.getString("status");
					if (currentStatus.contains("실패") == false && currentStatus.contains("취소") == false) {
						if (currentStatus.contains("다운로드") == false && currentStatus.contains("업로드") == false) {
							throw new Exception("[" + currentStatus + "] 항목은 요청을 진행할 수 없습니다");
						}
						
						try (PreparedStatement pstmt3 = conn.prepareStatement("UPDATE clip SET content_status='작업오류', status=?, last_time=now(3), note=?, worker=? WHERE cid=?"))  {
							pstmt3.setString(1, ChangeContentStatus.toFailStatus(currentStatus));
							pstmt3.setString(2, "I/O강제실패처리(" + note + ")");
							pstmt3.setString(3, worker);
							pstmt3.setString(4, cid);
							logger.info(pstmt3.toString());
							pstmt3.executeUpdate();
							DbHelper.insertClipHistroy(conn, "cid='" + cid + "'");
						}
						
						long sid = rs.getLong("sid");
						if( !rs.wasNull() ){
							if( currentStatus.contains("원본") ){						
								GalaxiaEncoderLoadBalancer.returnIdle( sid , 1);						
							}else if( currentStatus.toUpperCase().contains("TS") ){
								MspsLoadBalancer.returnIdle(sid, 1);
							}
						}
						
					}
				}
			}
			db.commit();

			ClipTaskManager.signal();

		} catch (Exception e) {
			db.rollback();
			logger.error("", e);
			jsonResponse.put("exception", EmsException.msg(e));
		} finally {
			db.close();
		}

		response.setCharacterEncoding("UTF-8");
		response.getWriter().print(jsonResponse);
		
		
	}
	
	private void ioAbortContent(HttpServletRequest request, HttpServletResponse response) throws IOException {
		JSONObject jsonResponse = new JSONObject();

		DbTransaction db = null;
		Connection conn = null;
		String sql = null;
		ArrayList<Define.IDItemEncAbort> listIDItem = new ArrayList<>();
		ArrayList<Define.IDItemVmaf> listStopVmaf = new ArrayList<Define.IDItemVmaf>();

		// 변경할 CID
		
		// SET 항목 같이 반영하지 말자
		// 실패처리되고 이후 작업 시  어차피 set  으로 묶여갈테니...

		try {
			String strContTaskStoreIP = null;
			String strURLVmafJob = null;
			String strURLVmaf = null;
			String strCid = request.getParameter("cid");
			String strScheType = request.getParameter("sche_type");
			String strWorker = Util.ConvNullIsEmptyStr( request.getParameter("worker") );
			String strNote = Util.ConvNullIsEmptyStr( request.getParameter("note") );
			
			if( strCid == null || strCid.isEmpty() ) {
				throw new Exception( "전달된 파라미터에 CID가 존재하지 않습니다." );
			}
			
			if( strScheType == null || strScheType.isEmpty() ) {
				throw new Exception( "전달된 파라미터에 스케줄 타입이 존재하지 않습니다. | " + strCid );
			}
			
			Define.IDType eIdType = common.Util.GetIDType( strScheType );			
			if( eIdType == null ) {
				throw new Exception( String.format( "스케줄 타입을 구하는데 실패하였습니다. | [ %s ] [ %s ]", strCid, strScheType ) );
			}
			
			if( eIdType == Define.IDType.RTSP ) {
				sql = "select * from cems.content where cid=?";
			}else if( eIdType == Define.IDType.HLS ) {
				sql = "select * from cems.content_hls_enc where cid=?";
			}else {
				throw new Exception( "알수 없는 스케줄 타입 파라미터 입니다. | " + strScheType );
			}
					
			db = new DbTransaction();
			conn = db.startAutoCommit();	
			try {
				try (PreparedStatement pstmt1 = conn.prepareStatement(sql)) {
					pstmt1.setString(1, strCid );
					try( ResultSet rs = pstmt1.executeQuery() ){
						if( rs.next() ){						
							long lSid = rs.getLong("sid");
							long lProfileId = rs.getLong("profile_id");
							String strStatus = rs.getString("status");
							String strType = rs.getString("type");
							String strFramesize = rs.getString("framesize");
							
							if( eIdType == Define.IDType.RTSP ) {
								
								if( strType != null && strType.toUpperCase().equals("MOBILE") ) {
									
									sql = "select * from cems.content where cid like ?";
									try (PreparedStatement pstmt2 = conn.prepareStatement(sql)) {
										pstmt2.setString(1, strCid + '%' );
										try( ResultSet rs2 = pstmt2.executeQuery() ){
											while( rs2.next() ){
												Define.IDItemEncAbort idItem = new Define.IDItemEncAbort();
												idItem.eIDType = Define.IDType.RTSP;
												idItem.strID = rs2.getString("cid");
												idItem.lProfileId = rs2.getLong("profile_id");
												idItem.lSid = rs2.getLong("sid");
												idItem.strStatus = rs2.getString("status");											
												listIDItem.add( idItem );
												
												if( rs2.getString("status") != null && ( rs2.getString("status").equals(Define.strStatus_Tasking_Raw_Vmaf_Wait) || rs2.getString("status").contains("[원본]VMAF(") )  ) {
													Define.IDItemVmaf stItemVmaf = new Define.IDItemVmaf( );
													stItemVmaf.strID = Util.ClearReplaceTag( rs2.getString("cid") );
													stItemVmaf.eIDType = Define.IDType.RTSP;
													stItemVmaf.strPathRaw = rs2.getString("1st_path_store_pertitle");
													stItemVmaf.strPathEncRet = rs2.getString("2nd_path_store_pertitle");
													listStopVmaf.add( stItemVmaf  );
				                            	}
												
											}
										}
									}
									
								}else {
									Define.IDItemEncAbort idItem = new Define.IDItemEncAbort();
									idItem.eIDType = Define.IDType.RTSP;
									idItem.strID = rs.getString("cid");
									idItem.lProfileId = rs.getLong("profile_id");
									idItem.lSid = rs.getLong("sid");
									idItem.strStatus = rs.getString("status");											
									listIDItem.add( idItem );
									
									if( rs.getString("status") != null && ( rs.getString("status").equals(Define.strStatus_Tasking_Raw_Vmaf_Wait) || rs.getString("status").contains("[원본]VMAF(") ) ) {
										Define.IDItemVmaf stItemVmaf = new Define.IDItemVmaf( );
										stItemVmaf.strID = Util.ClearReplaceTag( rs.getString("cid") );
										stItemVmaf.eIDType = Define.IDType.RTSP;
										stItemVmaf.strPathRaw = rs.getString("1st_path_store_pertitle");
										stItemVmaf.strPathEncRet = rs.getString("2nd_path_store_pertitle");										
										listStopVmaf.add( stItemVmaf );
	                            	}
								}
								
							}else {
								
								Define.IDItemEncAbort idItem = new Define.IDItemEncAbort();
								idItem.eIDType = Define.IDType.HLS;
								idItem.strHlsMediaID = rs.getString("mda_id");
								idItem.strID = rs.getString("cid");
								idItem.lProfileId = rs.getLong("profile_id");
								idItem.lSid = rs.getLong("sid");
								idItem.strStatus = rs.getString("status");											
								listIDItem.add( idItem );
								
								if( rs.getString("status") != null && ( rs.getString("status").equals(Define.strStatus_Tasking_Raw_Vmaf_Wait) || rs.getString("status").contains("[원본]VMAF(") )  ) {
									String strRsluMdaId = new DbCommonHelper().matchHLSMdaRsluID(conn, rs.getString("mda_id"), rs.getString("framesize"));
									if( strRsluMdaId != null && !strRsluMdaId.isEmpty() ) {
										Define.IDItemVmaf stItemVmaf = new Define.IDItemVmaf( );
										stItemVmaf.strID = strRsluMdaId;
										stItemVmaf.eIDType = Define.IDType.HLS;
										stItemVmaf.strPathRaw = rs.getString("1st_path_store_pertitle");
										stItemVmaf.strPathEncRet = rs.getString("2nd_path_store_pertitle");										
										listStopVmaf.add(  stItemVmaf );	
									}									
                            	}
								
							}
							
						}
						
					}
				}
				
				strContTaskStoreIP = new DbCommonHelper().GetConfigureDBValue(conn, "content_task_storage_ip" );
				strURLVmaf = new DbCommonHelper().GetConfigureDBValue(conn, Define.DBConfigureKey.pertitle_url_vmaf.toString());
				strURLVmafJob = new DbCommonHelper().GetConfigureDBValue(conn, Define.DBConfigureKey.pertitle_url_vmaf_job.toString());;
			}catch( Exception ex ) {
				logger.error("", ex);
				throw ex;
			}finally {
				db.close();
				conn = null;
			}
			
			
			for( Define.IDItemVmaf stIdItem : listStopVmaf ) {
				
				try {
					boolean bOK = false;
					
					do {
						
		                do {
		                	// Queued 중인 Job 삭제 시도
		                	RequestPertitleVmafStateJob reqVmafState = new RequestPertitleVmafStateJob.Builder(stIdItem.strID,stIdItem.eIDType, strURLVmafJob, null)
																								.setVmafStateCode( RequestPertitleVmafStateJob.VmafStateCode.queued )	
																								.build();
							if (!reqVmafState.action(null)) {
								throw new Exception(String.format("Vmaf stats Request Failure | [ ID : %s ] [ ID Type : %s ]", stIdItem.strID, stIdItem.eIDType.toString() ));
							}

		                    logger.info(String.format("Get Vmaf ID | [ ORi ID : %s ] [ Vmaf ID : %s ]", stIdItem.strID, reqVmafState.respID() ));
		                    
		                    if( Checker.isEmpty( reqVmafState.respID() ) ) {
		                    	logger.info( String.format("VMAF 대기중인 에서 작업ID가 조회가 조회되지 않습니다. VMAF Active Stop Proc으로 진행합니다. | [ %s ]", stIdItem.strID ) );
		                    	break;
		                    }
		                    
		                    RequestPertitleVmafRemove reqVmafRemove = new RequestPertitleVmafRemove.Builder(reqVmafState.respID(), stIdItem.eIDType, strURLVmafJob, null).build();
		                    if (!reqVmafRemove.action(null)) {
		                        throw new Exception(String.format("Vmaf Queued Remove 요청에 실패하였습니다. 네트워크 상태 또는 VMAF Agnet 확인이 필요합니다. | [ ID : %s ] [ ID Type : %s ] [ URL : %s ]",stIdItem.strID, stIdItem.eIDType.toString(), strURLVmafJob));
		                    }
		                    
		                    if(reqVmafRemove.respnStatusCode() == 200 ) {         
		                    	logger.info( String.format("VMAF JOB Qeueud Remove Success. | [ ID : %s ] [ Status Code : %d ]", stIdItem.strID, reqVmafRemove.respnStatusCode() ) );
		                    }else {
		                    	logger.info( String.format("Vmaf Queued Remove Status Code is Not Success Code, Next Active Stop Proc Start | [ URL : %s ] [ Status Code : %d ] [ ID : %s ]", strURLVmafJob, reqVmafRemove.respnStatusCode(), stIdItem.strID ) );
		                    	break;
		                    }
		                                    	
		                    bOK = true;
		                    
		                }while( false );
		                
		                if( bOK ) {
		                	logger.info( String.format("이미 VMAF JOB Qeueud Remove가 완료된 작업입니다. Active Stop Proc을 건너뜁니다. | [ ID : %s ]", stIdItem.strID ) );
		                	break;
		                }
						
		                RequestPertitleVmafStateJob reqVmafState = new RequestPertitleVmafStateJob.Builder( stIdItem.strID, stIdItem.eIDType, strURLVmafJob, null)
																								.setVmafStateCode(RequestPertitleVmafStateJob.VmafStateCode.active)
																								.build();
			            if (!reqVmafState.action(null)) {
			                throw new Exception(String.format("Vmaf stats Request Failure | [ ID : %s ] [ ID Type : %s ]",
			                									stIdItem.strID, 
			                									stIdItem.eIDType.toString()			                									
			                									));
			            }

			            logger.info(String.format("Get Vmaf ID | [ ORi ID : %s ] [ Vmaf ID : %s ]", stIdItem.strID, reqVmafState.respID() ));
			            
			            if( Checker.isEmpty( reqVmafState.respID() ) ) {
			            	throw new Exception( String.format("Vmaf 작업ID 조회결과가 존재하지 않습니다. VMAF에 확인이 필요합니다. | [ %s ]", stIdItem.strID ) );
			            }

			            RequestPertitleVmafStop reqVmafStop = new RequestPertitleVmafStop.Builder(reqVmafState.respID(), stIdItem.eIDType, strURLVmafJob, null).build();
			            if (!reqVmafStop.action(null)) {
			                throw new Exception(String.format("Vmaf Stop Request Failure | [ ID : %s ] [ ID Type : %s ] [ VMAF ID : %s ]",
			                								stIdItem.strID, 
			                								stIdItem.eIDType.toString(), 
			                								reqVmafState.respID()			                								
			                								));
			            }
			            
					}while( false );
					
				}catch( Exception ex ) {
					logger.error("",ex);
				}finally {
					try {
			            //파일삭제 - 원본
			            EMSProcServiceDeleteFileIoEx fileIo = new EMSProcServiceDeleteFileIoEx( stIdItem.strPathRaw, null );
			            fileIo.setSrcIp( strContTaskStoreIP );
			            fileIo.start(this);	
			            
			            //파일삭제 - 인코딩
			            fileIo = new EMSProcServiceDeleteFileIoEx( stIdItem.strPathEncRet, null );
			            fileIo.setSrcIp( strContTaskStoreIP );
			            fileIo.start(this);						
					}catch( Exception ex ) {
						logger.error("",ex);
					}	   
				}
				
			}				
				
			try {	
				db = new DbTransaction();
				conn = db.start();
				
				for( Define.IDItemEncAbort idItem : listIDItem ) {
					
					if( idItem.strStatus == null || idItem.strStatus.contains("실패") || idItem.strStatus.contains("오류") || idItem.strStatus.contains("취소") ) {
						logger.info( String.format("Alreay Error Status, This Schedule Changing Status Skip | [ IDType : %s ] [ ID : %s ] [ Status : %s ]", idItem.eIDType.toString(), idItem.strID, idItem.strStatus ) );
						continue;
					}
					
					String strStatusEdit = idItem.strStatus;
					int nPerscentIdxStart = strStatusEdit.lastIndexOf('(');
					if( nPerscentIdxStart > 0 ) {
						strStatusEdit = strStatusEdit.substring(0, nPerscentIdxStart );
					}
					
					strStatusEdit = strStatusEdit.replace("중", "") + "실패";
					
					if( idItem.eIDType == Define.IDType.RTSP ) {
						DbTransaction.updateDbSql( conn, String.format("update content set sid=null, content_status='작업오류', status='%s', last_time=now(3), note='강제실패처리', worker='%s' where cid='%s'", 
								strStatusEdit,
								strWorker,
								idItem.strID
								) );
						
						new ReportNCMS().ReportEpsdResolutionInfo( idItem.strID, ReportNCMS.STATUS_CODE.ErrorTask );
					}else if( idItem.eIDType == Define.IDType.HLS ) {
						DbTransaction.updateDbSql( conn, String.format("update content_hls_enc set sid=null, status='%s', last_time=now(3), note='강제실패처리', worker='%s' where cid='%s'", 
								strStatusEdit,
								strWorker,
								idItem.strID
								) );
						
						boolean bLastError = false;
										
						if( DbCommonHelper.IsHlsLastErrorCheckItem( conn, idItem.strHlsMediaID, idItem.strID ) ) {
							bLastError = true;	
						}														
								
						if( bLastError ) {
							DbTransaction.updateDbSql( conn, String.format( "UPDATE content_hls SET mda_matl_sts_cd='작업오류' WHERE mda_id='%s'", idItem.strHlsMediaID ) );
							DbTransaction.updateDbSql( conn, String.format("UPDATE content set content_status='작업오류', status='작업오류' WHERE cid='%s'", idItem.strHlsMediaID ));
							new ReportNCMSHls().mediaEncodingResult( conn, idItem.strHlsMediaID, ReportNCMS.STATUS_CODE.ErrorTask );	
						}else {
							logger.info( String.format( String.format("Is Not Last Error Hls Sche, Skip Task Error Matl Sts Cd Setting | [ %s ]", idItem.strHlsMediaID) ) );
						}
					}
					
					// 인코딩 중이었울 경우 인코더 반환을 한다
					long sid = idItem.lSid;
					long pid = idItem.lProfileId;
					
					if( strStatusEdit.contains("원본") && !strStatusEdit.contains("VMAF") ) {
								
						ProfileAssignInfo.ProfileInfo pInfo = ProfileAssignInfo.getProfileType(conn, pid);
//						int assign = ProfileAssignInfo.getAssignEx(conn, pInfo.type, pInfo.resolution, pInfo.strDivision );
						int assign = ProfileAssignInfo.getEncoderAssignEx(conn, pInfo.type, pInfo.resolution, pInfo.strDivision, sid );
						if( assign != -1 ){
																		
//							int nEncCopyTaskCnt = EncodingPipeline.IsEncCopyOutFileTaskCnt( conn, idItem.strID );
//							if( nEncCopyTaskCnt > 0 ) {
//								assign = ( assign * ( nEncCopyTaskCnt + 1 ) );
//							}
							
							LoadBalancerEncoderElemental.returnIdle(conn, sid, assign );
							int nAssignCur = LoadBalancerEncoderElemental.GetAssignCur( conn, sid );
							logger.info("=======================================================================");
							logger.info(String.format("Enc Assign Return OK | [ CID : %s ] [ Type : %s ] [ Framesize : %s ] [ SID : %d ] [ Return Assign : %d ] [ Cur Assign : %d ]",
									idItem.strID ,							
									pInfo.type,
									pInfo.resolution,
									sid, 
									assign, 
									nAssignCur 
									));
							logger.info("=======================================================================");
							
						} else {
							logger.error("[{}] ElementalEncoderLoadBalancer.returnIdle()", sid);
						}
						
					}else {
						MspsLoadBalancer.returnIdle( conn, sid, 1);
					}
					
				}
				
				db.commit();
			}catch( Exception ex ) {
				db.rollback();
				logger.error("",ex);
				throw ex;
			}finally {
				db.close();
			}
			
		} catch (Exception e) {
			logger.error("", e);
			jsonResponse.put("exception", EmsException.msg(e));
		} finally {
			TranscodingStatus.signal();
			BroadcastTaskManager.signal();
		}

		response.setCharacterEncoding("UTF-8");
		response.getWriter().print(jsonResponse);
		
	}
	
	@Override
	public void onFileIoSuccess(String src, String dest, Object... objs) {
		// TODO Auto-generated method stub
		try {
			logger.error( String.format("Delete File OK : %s", src ) );			
		}catch( Exception ex ) {
			logger.error("",ex);
		}
	}

	@Override
	public void onFileIoFailure(String src, String dest, Exception e, Object... objs) {
		// TODO Auto-generated method stub
		try {
			logger.error( String.format("Delete File Fail : %s", src ) );
		}catch( Exception ex ) {
			logger.error("",ex);
		}
	}
	

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
