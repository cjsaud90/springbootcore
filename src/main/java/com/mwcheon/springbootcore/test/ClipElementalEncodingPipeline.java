package com.mwcheon.springbootcore.test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import common.DbHelper;
import common.DbTransaction;
import common.EmsException;
import common.EmsPropertiesReader;
import common.ErrorCode;
import common.ExtraStatusManipulation;
import common.JsonValue;
import common.MountInfo;
import common.ProfileAssignInfo;
import common.Util;
import common.cms.ReportCMS;
import common.system.LoadBalancerEncoderElemental;
import common.system.SystemItem;
import controller.BroadcastTaskManager;
import controller.ClipTaskManager;
import controller.TranscodingStatus;
import controller.TranscodingTask;
import io.DeleteFileIo;
import io.EMSProcServiceDeleteFileIo;
import io.EMSProcServiceMoveFileIo;
import io.FileIoHandler;
import io.FtpFileIo;
import io.MoveFileIo;
import service.automation.ObservingEncodingInfo;
import task.TaskProcessorManager;
import task.pipeline.EncodingPipeline.EncodingItem;

public class ClipElementalEncodingPipeline extends EncodingPipeline implements FileIoHandler {
	private static Logger logger = LoggerFactory.getLogger(ClipElementalEncodingPipeline.class);
	
	
	// invoke from thread
	@Override
	public void start(String cid, Object... objs ) throws Exception {
		EncodingItem item = getItem("clip", cid);
		int totalAssign = (int)objs[0];
	
		logger.info("clip elemental encoding ready! cid=" + item.cid + ", sid=" + item.sid);
		
		ArrayList<EncodingItem> outputs = new ArrayList<EncodingItem>(){{ add(item); }};
		logger.info("clip elemental encoding output=" + outputs.size());

		boolean ok =false;
		
		// 장비 할당
		SystemItem system = LoadBalancerEncoderElemental.getSystemTbl().get(item.sid);
				
		try {
			Path encInput = null;			
			
			try {
				// [원본]다운로드 상태로 변경
				updateDownloadStatus("clip", outputs, system.id);
				
				ArrayList<Util.mountInfo> mountInfoList = MountInfo.Get();
				if( mountInfoList.size() <= 0 ){
					throw new EmsException(ErrorCode.MOUNTINFO_IS_NULL, "mount info is null" );
				}
				
				// 인코더 input 경로
				Path encOrigin = Paths.get(system.originPath);
				
				String from = null;
				String to = null;
				
				String inputFilename = Paths.get(item.srcPath).getFileName().toString();
				inputFilename = inputFilename.substring(0, inputFilename.lastIndexOf('.'));
								
				for (EncodingItem output : outputs) {					
					output.outputFilename = inputFilename;										
					output.outputPath = Paths.get(system.outputPath);
				}				
				
				// 원본 파일을 인코더 입수 경로에 복사
				from = Util.ConverteToNWDriveIP(item.srcPath, mountInfoList);
				to = "\\\\" + system.ip + encOrigin.resolve(Paths.get(item.srcPath).getFileName()).toString();
				String cidList = getEncodingParameterCidJsonTyeList( outputs );
				copyRemoteFile(from, to, "q=2&cid=" + cidList, cid);	
				encInput = encOrigin.resolve(Paths.get(item.srcPath).getFileName());
				
				// 인코더 입력폴더에 2일 전 파일은 그냥 지워버린다
				String delInputDaysAgo = "\\\\" + system.ip + encOrigin.toString().replace("/", "\\") + "^2";
				EMSProcServiceDeleteFileIo delIo = new EMSProcServiceDeleteFileIo(delInputDaysAgo, null);
				delIo.setSrcIp(Util.getCallingIP(from));
				delIo.start(null);
								
				DbTransaction.updateDbSql("UPDATE clip SET last_time=now(3), last_filepath='" + item.srcPath + "' WHERE 1st_filepath='" + item.srcPath + "'");
								
			} catch (Exception e) {
				DbTransaction.updateDbSql("UPDATE clip SET last_time=now(3), content_status='작업오류', status='[원본]다운로드실패', last_filepath='" + item.srcPath + "', note='" + Util.CuttingMaximumStr( e.getMessage(), 500 ) + "' WHERE 1st_filepath='" + item.srcPath + "'");
				DbHelper.insertClipHistroy("1st_filepath='" + item.srcPath + "'");
				throw e;
			}
			
			// request
			try {
				for (EncodingItem output : outputs) {
					TaskProcessorManager.addApiResultTbl(output.cid, this);
				}

				// 인코딩 요청
				for (int i = 0; i < 3; i++) {
					JSONObject response = requestEncoding(item.cid, system, encInput, outputs, false);
					int error = ((Long)response.get("error")).intValue();
					if (error != 0) {
						if (error == 507) {
							// 현재 해당 CID 작업을 시작하기전에 이전의 작업이 아직 남아있는 상태로 다시 시도한다.
							// 인코딩 상태에서 '작업대기'를 통해 인코딩중지API 호출 후 다시 인코딩 요청 시 transagent에서
							// 아직 인코딩 중지가 되지 않을 때 507 에러를 보내준다. 그럼 조금 기다렸다가 다시 요청한다.
							Thread.sleep(3000);
							continue;
						}
						throw new EmsException(ErrorCode.API_RESPONSE_ERROR, JsonValue.getString(response, "desc"));
					}
					break;
				}
				
				updateEncodingStatus("clip", "작업진행", "[원본]인코딩", system, outputs, "");
				
			} catch (Exception e) {
				Thread.sleep(1000);
				updateEncodingStatus("clip", "작업오류", "[원본]인코딩실패", system, outputs, e.getMessage());				
				throw e;
			}
			
			ok = true;

		} catch (Exception e) {
			ClipTaskManager.signal();
			
			for (EncodingItem output : outputs) {
				ReportCMS.statusClip(output.cid, "작업오류");
			}			
			throw e;
		} finally {
			if (ok == false) {
				LoadBalancerEncoderElemental.returnIdle(system.id, totalAssign);
			}
		}
	}
	
	@Override
	public void onResult(String cid, JSONObject jsn) throws Exception {
		long error = 0;
		long sid = 0;
		String filepath = null;
		String realFilepath = null;
		String inputpath = null;
		
		try {
			error = (long) JsonValue.get(jsn, "error");
			sid = Long.parseLong(JsonValue.getString(jsn, "sid"));
			filepath = JsonValue.getString(jsn, "filepath");
			realFilepath = JsonValue.getString(jsn, "encoder_output_filepath");		
			inputpath = JsonValue.getString(jsn, "inputpath");
			//"inputpath" : "/mnt/data/EMS/in/DS_302996_7D(영어자막)_5_[TV-HD]_161227.ts",
			
			// 트랜스코더 큐에 장비ID를 반환한다.
			Long pid = ProfileAssignInfo.getProfileID("clip", cid );
			if(pid != null){
				ProfileAssignInfo.ProfileInfo pInfo = ProfileAssignInfo.getProfileType(pid);
//				int assign = ProfileAssignInfo.getAssign(pInfo.type, pInfo.resolution, pInfo.strDivision );
				int assign = ProfileAssignInfo.getAssign(pInfo.type, pInfo.resolution, pInfo.strDivision, sid );
				if( assign != -1 ){
					LoadBalancerEncoderElemental.returnIdle(sid, assign);								
					int nAssignCur = LoadBalancerEncoderElemental.GetAssignCur( null, sid );
					logger.info("=======================================================================");
					logger.info(String.format("Clip Enc Assign Return OK | [ CID : %s ] [ Type : %s ] [ Framesize : %s ] [ SID : %d ] [ Return Assign : %d ] [ Cur Assign : %d ]",
							cid,							
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
			}else{
				throw new EmsException(ErrorCode.INVALID_VALUE_PROPERTY, String.format("result pid is not exist | [tbl : moblie ] [ cid : %s ]", cid));
			}

			if (error != 0) {
				throw new EmsException(ErrorCode.API_RESPONSE_ERROR, JsonValue.getString(jsn, "encoder_err"));
			}
			
			String filename = Paths.get(filepath).getFileName().toString();
			String filenameWoExt = filename.substring(0, filename.lastIndexOf('.'));
				
			DbTransaction.updateDbSql("UPDATE clip SET last_time=now(3), content_status='작업진행', status='[원본]인코딩파일이동', mapped_filename='" + filenameWoExt +"' WHERE cid='" + cid + "'");
			DbHelper.insertClipHistroy("cid='" + cid + "'");
			ClipTaskManager.signal();
			
			SystemItem system = LoadBalancerEncoderElemental.getSystemTbl().get(sid);
			
			ArrayList<Util.mountInfo> mountInfoList = MountInfo.Get();
			if( mountInfoList.size() <= 0 ){
				throw new EmsException(ErrorCode.MOUNTINFO_IS_NULL, "mount info is null" );
			}
			
			// TS 작업시작 폴더로 이동						
			String from = "\\\\" + system.ip + realFilepath.substring(4/*/mnt*/).replace("/", "\\");								
			Path dest = Paths.get(EmsPropertiesReader.get("[클립TS]시작.io.dest"));
			dest = dest.resolve(filename);
							
			String to = Util.ConverteToNWDriveIP(dest.toString(), mountInfoList);
			EMSProcServiceMoveFileIo fileIo = new EMSProcServiceMoveFileIo(from, to);
			fileIo.setSrcIp(Util.getCallingIP(to));
			fileIo.setServletParam("q=3&cid=" + cid);
			fileIo.start(this, cid, dest.toString() );
								
			// 원본파일 삭제
			Path encOrigin = Paths.get(system.originPath);
			String delfilename = Paths.get(inputpath).getFileName().toString();
			String delpath = "\\\\" + system.ip + encOrigin.resolve(delfilename).toString().replace('/', '\\');
			EMSProcServiceDeleteFileIo delIo = new EMSProcServiceDeleteFileIo(delpath, null);
			delIo.setSrcIp(Util.getCallingIP(to));
			delIo.start(null);

		} catch (Exception e) {
			DbTransaction.updateDbSql("UPDATE clip SET last_time=now(3), content_status='작업오류', status='[원본]인코딩실패', note='" + Util.CuttingMaximumStr( e.getMessage(), 500 ) + "' WHERE cid='" + cid + "'");
			DbHelper.insertClipHistroy("cid='" + cid + "'");
			ReportCMS.statusClip(cid, "작업오류");					
			throw e;
		} finally {
			ClipTaskManager.signal();
		}		
	}
	
	
	
	@Override
	public void onFileIoSuccess(String src, String dest, Object... objects) {
		String cid = (String) objects[0];
		logger.info("file i/o success / src=" + src + " / dest=" + dest + " / cid=" + cid);	
	}

	@Override
	public void onFileIoFailure(String src, String dest, Exception e, Object... objects) {
		String cid = (String) objects[0];
		logger.info("file i/o failure / src=" + src + " / dest=" + dest + " / cid=" + cid);
		try {
			DbTransaction.updateDbSql("UPDATE clip SET last_time=now(3), content_status='작업오류', status='[원본]인코딩파일이동실패' WHERE cid='" + cid + "'");
			DbHelper.insertClipHistroy("cid='" + cid + "'");
			ClipTaskManager.signal();
		} catch (Exception e1) {
			logger.error("", e1);
		}
	}
	
	protected EncodingItem getItem(String tbl, String cid) throws Exception {
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		try {
			conn = db.startAutoCommit();
			try (PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM " + tbl + " WHERE cid=?")) {
				pstmt.setString(1, cid);
				ResultSet rs = pstmt.executeQuery();
				if (rs.next()) {
					EncodingItem item = new EncodingItem();
					item.cid = rs.getString("cid");
					item.sid = rs.getLong("sid");
					item.srcPath = rs.getString("1st_filepath");
					long lExtraStatus = rs.getLong("extra_status");
					if( ( lExtraStatus & ExtraStatusManipulation.Flag.EF_NEW_DRM.get() ) != 0 || ( lExtraStatus & ExtraStatusManipulation.Flag.EF_WATERMARK.get() ) != 0 ){
						item.strExt = "mp4";
					}else{
						item.strExt = "ts";
					}
					return item;
				}
			}
		} finally {
			db.close();
		}
		return null;
	}
}
