package com.mwcheon.springbootcore.test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.GsonBuilder;

import common.Checker;
import common.ConvIP;
import common.ConvIP.ConvIPFlag;
import common.ConvIP.ConvIPReplaceFlag;
import common.DbCommonHelper;
import common.DbHelper;
import common.DbTransaction;
import common.Define;
import common.Define.IDItem;
import common.Define.IDType;
import common.DomaincodeConversion;
import common.EmsException;
import common.EmsPropertiesReader;
import common.ErrorCode;
import common.ExtraStatusManipulation;
import common.HttpRESTful;
import common.JsonValue;
import common.MappingFilenameGen;
import common.MountInfo;
import common.ProfileAssignInfo;
import common.TaskReqReplaceIP;
import common.TaskReqReplaceIPSupernova;
import common.Util;
import common.WatermarkDrmInfo;
import common.Util.contIDInfo;
import common.ncms.ReportNCMS;
import common.ncms.ReportNCMSHls;
import common.pertitle.RequestPertitleMediaFanAGT;
import common.pertitle.RequestPertitleMediaFanAGT.IFType;
import common.system.LoadBalancerEncoderElemental;
import common.system.SystemItem;
import controller.BroadcastTaskManager;
import controller.ClipTaskManager;
import controller.HawkEyeFIleUploader;
import controller.HawkEyeManager;
import controller.SetNumberOnOff;
import controller.TranscodingStatus;
import controller.TranscodingTask;
import controller.SetNumberOnOff.checkSameHighest;
import io.CopyFileIo;
import io.EMSProcServiceCopyFileIo;
import io.EMSProcServiceDeleteFileIo;
import io.FileIo;
import io.FileIoHandler;
import io.MoveFileIo;
import service.automation.ObservingQVAL;
import task.pipeline.EncodingPipeline.EncCompleteItem;
import task.pipeline.EncodingPipeline.EncodingItem;

public abstract class EncodingPipeline implements Pipeline {
	private static Logger logger = LoggerFactory.getLogger(EncodingPipeline.class);	
	
	class EncodingItem {
		boolean bHlsVttExtraction;
		ArrayList<String> listHlsCaptLagFgCdCc = new ArrayList<>();
		boolean bExistCaptionTask;
		Define.IDType eIDType;
		String strHlsMediaId;
		String strHlsEqualRtspCid;
		String strPossnYn;
		String cid;
		String srcPath;
		Long sid; // 예약장비
		String copyright;	
		//String content_name;
		//String channel;
		String series;
		String smiPath;
		String cvtSmiPath;
		
		// internal
		Path outputPath;
		String strEpadId;
		String outputFilename;
		String cautionPath;
		String logoPath;
		String framesize;
		String ratio;
		
		long extra_status;
		
		//Ext
		String strExt;
		String strType;		
		
		int nSubUsage;
		String strRawTaskCaptionFileName;
		String capt_bas_lag_fg_cd; //ecdn 운영시의 기본자막
		String mtx_bas_lag_fg_cd; //ecdn에서 멀티 자막을 위한 필드
		//boolean bEcdnUse;
		boolean bUseDummyEnc;
		
		ArrayList<Object[]> listAudioInfo = new ArrayList<>();
		
		long lEncOptHdrId;
		
		//OptSn
		int nOptSnAssign;
		int nOptSnQuality;		
		
		EncodingItem()
		{
			bHlsVttExtraction = false;
			bExistCaptionTask = false;
			bUseDummyEnc = false;
			nSubUsage = 0;
			//bEcdnUse = false;	
			extra_status = 0;
			lEncOptHdrId = -1;
			nOptSnAssign = 0;
			nOptSnQuality = 0;			
		}
	}
	
	//ECDN에서 다중자막 처리를 위함이다.
	class ECDN_MTX_CAPT{
		String mtx_capt_yn;
		String mtx_capt_bas_lag_fg_cd;
		String mtx_capt_lag_fg_cd;
	}

	protected boolean CheckSameEncoderType( ArrayList<EncodingItem> outputs )throws Exception
	{
		boolean bRet = true;
		
		try {
			if( outputs == null || outputs.isEmpty() ) {
				throw new Exception( "Param outputs is Empty" );
			}
			
			HashMap<Integer, String> mapEncType = new HashMap<Integer, String>();
			
			for( EncodingItem stItem : outputs ) {
				if( stItem.nOptSnAssign == Define.EncdPiquTypCd.Supernova.get() ) {
					mapEncType.put( Define.EncoderType.SUPERNOVA.get(), "1" );
				}else{
					mapEncType.put( Define.EncoderType.ELEMENTAL.get(), "1" );
				}
			}
			
			if( mapEncType.size() != 1 ) {
				bRet = false;
				logger.info( String.format("Do Assign, Not Same Encoder Type Schedule, | [ mapEncType Cnt : %d ]", mapEncType.size() ) );
			}
			
		}catch( Exception ex ) {
			logger.error("",ex);
			throw ex;
		}
		
		return bRet;
	}
	
	protected String GetEpsdID( String strCID ) throws Exception
	{
		String strRet = null;
		
		try{
			if( strCID == null || strCID.isEmpty() ){
				throw new Exception( "CID is empty" );
			}
			
			DbTransaction db = new DbTransaction();
			Connection conn = null;
			try {
				conn = db.startAutoCommit();
				try (PreparedStatement pstmt = conn.prepareStatement("SELECT epsd_id FROM cems.content WHERE cid=?")) {
					pstmt.setString(1, strCID);
					try( ResultSet rs = pstmt.executeQuery() ){
						if (rs.next()) {
							strRet = rs.getString(1);
						}
					}
				}
			}catch( Exception ex2 ){
				logger.error("",ex2);
				throw ex2;
			} finally {
				db.close();
			}
			
		}catch( Exception ex ){
			logger.error("",ex);
			throw ex;
		}
		
		return strRet;
	}
	
	protected enum CopyKey { Input, Logo, Caution, Smi };
	
	protected EncodingItem getItem(String tbl, String cid) throws Exception {
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		try {
			conn = db.startAutoCommit();
			try (PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM " + tbl + " WHERE cid=?")) {
				pstmt.setString(1, cid);
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {
						EncodingItem item = new EncodingItem();
						item.cid = rs.getString("cid");
						item.sid = rs.getLong("sid");
						//item.content_name = rs.getString("content_name");
						//item.channel = rs.getString("channel");
						item.series = String.valueOf( rs.getInt("series") );
						item.srcPath = rs.getString("1st_filepath");
						item.copyright = rs.getString("copyright");
						item.smiPath = rs.getString("sub_upload_filepath");						
						
						return item;
					}
				}
			}
		} finally {
			db.close();
		}
		return null;
	}
	
	//맨 처음 콘탠츠 정보를 기준으로 가져온다.
	protected EncodingItem getItem(String tbl, ArrayList<Util.SystemAssignInfo> contInfoList) throws Exception {
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		try {
			if( contInfoList == null || contInfoList.size() <= 0 ){
				throw new EmsException(ErrorCode.INVALID_VALUE_PROPERTY, "contInfo List is empty");
			}
			
			conn = db.startAutoCommit();
			try (PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM " + tbl + " WHERE cid=?")) {
				pstmt.setString(1, contInfoList.get(0).cid);
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {
						EncodingItem item = new EncodingItem();
						item.cid = rs.getString("cid");
						item.sid = rs.getLong("sid");
						//item.content_name = rs.getString("content_name");
						//item.channel = rs.getString("channel");
						item.series = String.valueOf( rs.getInt("series") );
						item.srcPath = rs.getString("1st_filepath");
						item.copyright = rs.getString("copyright");
						item.smiPath = rs.getString("sub_upload_filepath");
						return item;
					}
				}
			}
		} finally {
			db.close();
		}
		return null;
	}
	
	//다중자막리스트 값 가져오기
	protected String getSublanglist(String cid) {
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		String sResult = "";
		try {
			conn = db.start();			
			String sql = null;			
			sql = "SELECT sub_lang_list FROM content WHERE cid=?";
				
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setString(1, cid);
				logger.info(pstmt.toString());
				ResultSet rs = pstmt.executeQuery();
				if (rs.next()) {
					String sublanglist = rs.getString("sub_lang_list");	
				
					sResult = sublanglist;
				}
			}
				
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			db.close();
		}
		return sResult;
	}
		
//	protected ArrayList<EncodingItem> getOutputs(String tbl, String cid) {
//		ArrayList<EncodingItem> itemList = new ArrayList<>();
//
//		// cid 로 조회해서 SET 으로 묶여있으면 묶여있는 항목을 모두 변환한다.
//
//		DbTransaction db = new DbTransaction();
//		Connection conn = null;
//
//		try {
//			conn = db.start();			
//			String sql = null;			
//			sql = "SELECT * FROM " + tbl + " WHERE cid=?";
//			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
//				pstmt.setString(1, cid);
//				try( ResultSet rs = pstmt.executeQuery() ){
//					if (rs.next()) {				
//						String setNum = rs.getString("set_num");
//						String strPossnYN = rs.getString("possn_yn");
//						if (setNum != null && setNum.length() > 0) {
//							sql = "SELECT * FROM " + tbl + " WHERE set_num=? AND ((extra_status & ?) = 0)";
//							try (PreparedStatement pstmt2 = conn.prepareStatement(sql)) {
//								pstmt2.setString(1, setNum);
//								pstmt2.setInt(2, ExtraStatusManipulation.Flag.EF_NO_DISPLAY.get());
//								try( ResultSet rs2 = pstmt2.executeQuery() ){					
//									while (rs2.next()) {
//										EncodingItem item = new EncodingItem();
//										item.cid = rs2.getString("cid");
//										item.content_name = rs2.getString("content_name");
//										item.copyright = rs2.getString("copyright");
//										item.channel = rs2.getString("channel");
//										item.series = String.valueOf( rs2.getInt("series") );
//										item.extra_status = rs2.getLong("extra_status");																				
//										item.nSubUsage = rs2.getInt("sub_usage");
//										item.framesize = rs2.getString("framesize");
//										item.bEcdnUse = Util.ConvNullIsEmptyStr( rs2.getString("ecdn_yn") ).toUpperCase().equals( "Y" ) ? true : false;
//										
//										long lExtraStatus = rs2.getLong("extra_status");
//										if( ( lExtraStatus & ExtraStatusManipulation.Flag.EF_NEW_DRM.get() ) != 0 || 
//												( lExtraStatus & ExtraStatusManipulation.Flag.EF_WATERMARK.get() ) != 0 ||
//												( lExtraStatus & ExtraStatusManipulation.Flag.EF_ADD_ECDN_TASK.get() ) != 0 ){
//											if( item.framesize.equals("UHD") || item.framesize.equals("UHD+HDR") ) {
//												
//												item.strExt = "ts";
//											} else {
//												item.strExt = "mp4";
//											}
//										}else{
//											item.strExt = "ts";
//										}
//										
//										if( tbl.toLowerCase().equals("content") ){
//											item.strType = rs.getString("type");
//										}
//										
//										item.strRawTaskCaptionFileName = rs.getString("sub_raw_task_filename");
//										
//										item.capt_bas_lag_fg_cd = rs2.getString("sub_base_lang");
//										if( item.capt_bas_lag_fg_cd != null && !item.capt_bas_lag_fg_cd.isEmpty() ) {
//											item.capt_bas_lag_fg_cd = DomaincodeConversion.ConverteLangEcdn( item.capt_bas_lag_fg_cd );
//										}
//										
//										item.mtx_bas_lag_fg_cd = rs2.getString("sub_lang_def");
//										if( item.mtx_bas_lag_fg_cd != null && !item.mtx_bas_lag_fg_cd.isEmpty() ) {
//											item.mtx_bas_lag_fg_cd = DomaincodeConversion.ConverteLangEcdn( item.mtx_bas_lag_fg_cd );
//										}
//										
//										itemList.add(item);
//									}
//								}
//							}
//						}else {
//							//Watermark까지는 주석처리된 코드로 있었다.
////							EncodingItem item = new EncodingItem();
////							item.cid = rs.getString("cid");
////							item.content_name = rs.getString("content_name");
////							item.copyright = rs.getString("copyright");
////							item.channel = rs.getString("channel");
////							item.series = String.valueOf( rs.getInt("series") );
////							item.extra_status = rs.getLong("extra_status");													
////							item.nSubUsage = rs.getInt("sub_usage");
////							item.framesize = rs.getString("framesize");
////							
////							long lExtraStatus = rs.getLong("extra_status");
////							if( ( lExtraStatus & ExtraStatusManipulation.Flag.EF_NEW_DRM.get() ) != 0 || ( lExtraStatus & ExtraStatusManipulation.Flag.EF_WATERMARK.get() ) != 0 ){
////								item.strExt = "mp4";
////							}else{
////								item.strExt = "ts";
////							}
////							
////							if( tbl.toLowerCase().equals("content") ){
////								item.strType = rs.getString("type");
////							}
////							
////							item.strRawTaskCaptionFileName = rs.getString("sub_raw_task_filename");
////							
////							itemList.add(item);
//							sql = "SELECT * FROM " + tbl + " WHERE UPPER(type)='TV' AND status='[원본]작업준비' AND epsd_id=? and possn_yn=?";
//							try( PreparedStatement pstmt2 = conn.prepareStatement(sql) ) {
//								pstmt2.setString(1, rs.getString("epsd_id"));
//								pstmt2.setString(2, strPossnYN );								
//								try(ResultSet rs2 = pstmt2.executeQuery() ) {
//									while(rs2.next() ) {
//										do {
//											EncodingItem item = new EncodingItem();
//											item.cid = rs2.getString("cid");
//											item.content_name = rs2.getString("content_name");
//											item.copyright = rs2.getString("copyright");
//											item.channel = rs2.getString("channel");
//											item.series = String.valueOf( rs2.getInt("series") );
//											item.extra_status = rs2.getLong("extra_status");
//											
//											int subUsage = rs2.getInt("sub_usage");
//											if (subUsage == 1) {
//												item.smiPath = rs2.getString("sub_upload_filepath");
//											}
//											item.nSubUsage = rs2.getInt("sub_usage");
//											item.framesize = rs2.getString("framesize");
//											item.bEcdnUse = Util.ConvNullIsEmptyStr( rs2.getString("ecdn_yn") ).toUpperCase().equals( "Y" ) ? true : false;
//											
//											long lExtraStatus = rs2.getLong("extra_status");
//											if( ( lExtraStatus & ExtraStatusManipulation.Flag.EF_ADD_ECDN_TASK.get() ) != 0 ){
//												if( item.framesize.equals("UHD") || item.framesize.equals("UHD+HDR") ) {
//													
//													item.strExt = "ts";
//												} else {
//													item.strExt = "mp4";
//												}
//											}else{
//												item.strExt = "ts";
//											}
//											
////											if( tbl.toLowerCase().equals("content") ){
////												item.strType = rs2.getString("type");
////											}
//											
//											item.strRawTaskCaptionFileName = rs2.getString("sub_raw_task_filename");
//											item.capt_bas_lag_fg_cd = rs2.getString("sub_base_lang");
//											if( item.capt_bas_lag_fg_cd != null && !item.capt_bas_lag_fg_cd.isEmpty() ) {
//												item.capt_bas_lag_fg_cd = DomaincodeConversion.ConverteLangEcdn( item.capt_bas_lag_fg_cd );
//											}
//											
//											item.mtx_bas_lag_fg_cd = rs2.getString("sub_lang_def");
//											if( item.mtx_bas_lag_fg_cd != null && !item.mtx_bas_lag_fg_cd.isEmpty() ) {
//												item.mtx_bas_lag_fg_cd = DomaincodeConversion.ConverteLangEcdn( item.mtx_bas_lag_fg_cd );
//											}
//											
//											itemList.add(item);
//										} while(rs.next());
//									}
//								}
//
//							}
//						}
//					}
//				}
//			}
//			db.commit();
//			return itemList;
//		} catch (Exception e) {
//			logger.error("", e);
//		} finally {
//			db.close();
//		}
//		return null;
//	}
	
	
//	protected ArrayList<EncodingItem> getOutputs(String tbl, String cid) {
//		ArrayList<EncodingItem> itemList = new ArrayList<>();
//
//		// cid 로 조회해서 SET 으로 묶여있으면 묶여있는 항목을 모두 변환한다.
//
//		DbTransaction db = new DbTransaction();
//		Connection conn = null;
//
//		try {
//			conn = db.start();			
//			String sql = null;			
//			sql = "SELECT * FROM " + tbl + " WHERE cid=?";
//			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
//				pstmt.setString(1, cid);
//				try( ResultSet rs = pstmt.executeQuery() ){
//					if (rs.next()) {				
//						String setNum = rs.getString("set_num");
//						String strPossnYN = rs.getString("possn_yn");
//						if (setNum != null && setNum.length() > 0) {
//							sql = "SELECT * FROM " + tbl + " WHERE set_num=? AND ((extra_status & ?) = 0)";
//							try (PreparedStatement pstmt2 = conn.prepareStatement(sql)) {
//								pstmt2.setString(1, setNum);
//								pstmt2.setInt(2, ExtraStatusManipulation.Flag.EF_NO_DISPLAY.get());
//								try( ResultSet rs2 = pstmt2.executeQuery() ){					
//									while (rs2.next()) {
//										EncodingItem item = new EncodingItem();
//										item.cid = rs2.getString("cid");
//										//item.content_name = rs2.getString("content_name");
//										item.copyright = rs2.getString("copyright");
//										//item.channel = rs2.getString("channel");
//										item.series = String.valueOf( rs2.getInt("series") );
//										item.extra_status = rs2.getLong("extra_status");																				
//										item.nSubUsage = rs2.getInt("sub_usage");
//										item.framesize = rs2.getString("framesize");
//										//item.bEcdnUse = Util.ConvNullIsEmptyStr( rs2.getString("ecdn_yn") ).toUpperCase().equals( "Y" ) ? true : false;
//										
//										long lExtraStatus = rs2.getLong("extra_status");
//										if( ( lExtraStatus & ExtraStatusManipulation.Flag.EF_NEW_DRM.get() ) != 0 || 
//												( lExtraStatus & ExtraStatusManipulation.Flag.EF_WATERMARK.get() ) != 0 ||
//												( lExtraStatus & ExtraStatusManipulation.Flag.EF_ADD_ECDN_TASK.get() ) != 0 ){
//											if( item.framesize.equals("UHD") || item.framesize.equals("UHD+HDR") ) {
//												
//												item.strExt = "ts";
//											} else {
//												item.strExt = "mp4";
//											}
//										}else{
//											item.strExt = "ts";
//										}
//										
//										if( tbl.toLowerCase().equals("content") ){
//											item.strType = rs.getString("type");
//										}
//										
//										item.strRawTaskCaptionFileName = rs.getString("sub_raw_task_filename");
//										
//										item.capt_bas_lag_fg_cd = rs2.getString("sub_base_lang");
//										if( item.capt_bas_lag_fg_cd != null && !item.capt_bas_lag_fg_cd.isEmpty() ) {
//											item.capt_bas_lag_fg_cd = DomaincodeConversion.ConverteLangEcdn( item.capt_bas_lag_fg_cd );
//										}
//										
//										item.mtx_bas_lag_fg_cd = rs2.getString("sub_lang_def");
//										if( item.mtx_bas_lag_fg_cd != null && !item.mtx_bas_lag_fg_cd.isEmpty() ) {
//											item.mtx_bas_lag_fg_cd = DomaincodeConversion.ConverteLangEcdn( item.mtx_bas_lag_fg_cd );
//										}
//										
//										itemList.add(item);
//									}
//								}
//							}
//						}else {
//							sql = "SELECT * FROM " + tbl + " WHERE UPPER(type)='TV' AND status='[원본]작업준비' AND epsd_id=? and possn_yn=?";
//							try( PreparedStatement pstmt2 = conn.prepareStatement(sql) ) {
//								pstmt2.setString(1, rs.getString("epsd_id"));
//								pstmt2.setString(2, strPossnYN );								
//								try(ResultSet rs2 = pstmt2.executeQuery() ) {
//									while(rs2.next() ) {
//										do {
//											EncodingItem item = new EncodingItem();
//											item.cid = rs2.getString("cid");
//											//item.content_name = rs2.getString("content_name");
//											item.copyright = rs2.getString("copyright");
//											//item.channel = rs2.getString("channel");
//											item.series = String.valueOf( rs2.getInt("series") );
//											item.extra_status = rs2.getLong("extra_status");
//											
//											int subUsage = rs2.getInt("sub_usage");
//											if (subUsage == 1) {
//												item.smiPath = rs2.getString("sub_upload_filepath");
//											}
//											item.nSubUsage = rs2.getInt("sub_usage");
//											item.framesize = rs2.getString("framesize");
//											//item.bEcdnUse = Util.ConvNullIsEmptyStr( rs2.getString("ecdn_yn") ).toUpperCase().equals( "Y" ) ? true : false;
//																						
//											item.strExt = "ts";											
//											
//											item.strRawTaskCaptionFileName = rs2.getString("sub_raw_task_filename");
//											item.capt_bas_lag_fg_cd = rs2.getString("sub_base_lang");
//											if( item.capt_bas_lag_fg_cd != null && !item.capt_bas_lag_fg_cd.isEmpty() ) {
//												item.capt_bas_lag_fg_cd = DomaincodeConversion.ConverteLangEcdn( item.capt_bas_lag_fg_cd );
//											}
//											
//											item.mtx_bas_lag_fg_cd = rs2.getString("sub_lang_def");
//											if( item.mtx_bas_lag_fg_cd != null && !item.mtx_bas_lag_fg_cd.isEmpty() ) {
//												item.mtx_bas_lag_fg_cd = DomaincodeConversion.ConverteLangEcdn( item.mtx_bas_lag_fg_cd );
//											}
//											
//											itemList.add(item);
//										} while(rs.next());
//									}
//								}
//
//							}
//						}
//					}
//				}
//			}
//			db.commit();
//			return itemList;
//		} catch (Exception e) {
//			logger.error("", e);
//		} finally {
//			db.close();
//		}
//		return null;
//	}
	
	
//	protected ArrayList<EncodingItem> getOutputs( String cid ) {
//		ArrayList<EncodingItem> itemList = new ArrayList<>();
//
//		// cid 로 조회해서 SET 으로 묶여있으면 묶여있는 항목을 모두 변환한다.
//
//		DbTransaction db = new DbTransaction();
//		Connection conn = null;
//
//		try {
//			conn = db.start();			
//			String sql = null;			
//			sql = "SELECT * FROM " + tbl + " WHERE cid=?";
//			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
//				pstmt.setString(1, cid);
//				try( ResultSet rs = pstmt.executeQuery() ){
//					if (rs.next()) {				
//						String setNum = rs.getString("set_num");
//						String strPossnYN = rs.getString("possn_yn");
//						if (setNum != null && setNum.length() > 0) {
//							sql = "SELECT * FROM " + tbl + " WHERE set_num=? AND ((extra_status & ?) = 0)";
//							try (PreparedStatement pstmt2 = conn.prepareStatement(sql)) {
//								pstmt2.setString(1, setNum);
//								pstmt2.setInt(2, ExtraStatusManipulation.Flag.EF_NO_DISPLAY.get());
//								try( ResultSet rs2 = pstmt2.executeQuery() ){					
//									while (rs2.next()) {
//										EncodingItem item = new EncodingItem();
//										item.cid = rs2.getString("cid");
//										//item.content_name = rs2.getString("content_name");
//										item.copyright = rs2.getString("copyright");
//										//item.channel = rs2.getString("channel");
//										item.series = String.valueOf( rs2.getInt("series") );
//										item.extra_status = rs2.getLong("extra_status");																				
//										item.nSubUsage = rs2.getInt("sub_usage");
//										item.framesize = rs2.getString("framesize");
//										//item.bEcdnUse = Util.ConvNullIsEmptyStr( rs2.getString("ecdn_yn") ).toUpperCase().equals( "Y" ) ? true : false;
//										
//										long lExtraStatus = rs2.getLong("extra_status");
//										if( ( lExtraStatus & ExtraStatusManipulation.Flag.EF_NEW_DRM.get() ) != 0 || 
//												( lExtraStatus & ExtraStatusManipulation.Flag.EF_WATERMARK.get() ) != 0 ||
//												( lExtraStatus & ExtraStatusManipulation.Flag.EF_ADD_ECDN_TASK.get() ) != 0 ){
//											if( item.framesize.equals("UHD") || item.framesize.equals("UHD+HDR") ) {
//												
//												item.strExt = "ts";
//											} else {
//												item.strExt = "mp4";
//											}
//										}else{
//											item.strExt = "ts";
//										}
//										
//										if( tbl.toLowerCase().equals("content") ){
//											item.strType = rs.getString("type");
//										}
//										
//										item.strRawTaskCaptionFileName = rs.getString("sub_raw_task_filename");
//										
//										item.capt_bas_lag_fg_cd = rs2.getString("sub_base_lang");
//										if( item.capt_bas_lag_fg_cd != null && !item.capt_bas_lag_fg_cd.isEmpty() ) {
//											item.capt_bas_lag_fg_cd = DomaincodeConversion.ConverteLangEcdn( item.capt_bas_lag_fg_cd );
//										}
//										
//										item.mtx_bas_lag_fg_cd = rs2.getString("sub_lang_def");
//										if( item.mtx_bas_lag_fg_cd != null && !item.mtx_bas_lag_fg_cd.isEmpty() ) {
//											item.mtx_bas_lag_fg_cd = DomaincodeConversion.ConverteLangEcdn( item.mtx_bas_lag_fg_cd );
//										}
//										
//										itemList.add(item);
//									}
//								}
//							}
//						}else {
//							sql = "SELECT * FROM " + tbl + " WHERE UPPER(type)='TV' AND status='[원본]작업준비' AND epsd_id=? and possn_yn=?";
//							try( PreparedStatement pstmt2 = conn.prepareStatement(sql) ) {
//								pstmt2.setString(1, rs.getString("epsd_id"));
//								pstmt2.setString(2, strPossnYN );								
//								try(ResultSet rs2 = pstmt2.executeQuery() ) {
//									while(rs2.next() ) {
//										do {
//											EncodingItem item = new EncodingItem();
//											item.cid = rs2.getString("cid");
//											//item.content_name = rs2.getString("content_name");
//											item.copyright = rs2.getString("copyright");
//											//item.channel = rs2.getString("channel");
//											item.series = String.valueOf( rs2.getInt("series") );
//											item.extra_status = rs2.getLong("extra_status");
//											
//											int subUsage = rs2.getInt("sub_usage");
//											if (subUsage == 1) {
//												item.smiPath = rs2.getString("sub_upload_filepath");
//											}
//											item.nSubUsage = rs2.getInt("sub_usage");
//											item.framesize = rs2.getString("framesize");
//											//item.bEcdnUse = Util.ConvNullIsEmptyStr( rs2.getString("ecdn_yn") ).toUpperCase().equals( "Y" ) ? true : false;
//																						
//											item.strExt = "ts";											
//											
//											item.strRawTaskCaptionFileName = rs2.getString("sub_raw_task_filename");
//											item.capt_bas_lag_fg_cd = rs2.getString("sub_base_lang");
//											if( item.capt_bas_lag_fg_cd != null && !item.capt_bas_lag_fg_cd.isEmpty() ) {
//												item.capt_bas_lag_fg_cd = DomaincodeConversion.ConverteLangEcdn( item.capt_bas_lag_fg_cd );
//											}
//											
//											item.mtx_bas_lag_fg_cd = rs2.getString("sub_lang_def");
//											if( item.mtx_bas_lag_fg_cd != null && !item.mtx_bas_lag_fg_cd.isEmpty() ) {
//												item.mtx_bas_lag_fg_cd = DomaincodeConversion.ConverteLangEcdn( item.mtx_bas_lag_fg_cd );
//											}
//											
//											itemList.add(item);
//										} while(rs.next());
//									}
//								}
//
//							}
//						}
//					}
//				}
//			}
//			db.commit();
//			return itemList;
//		} catch (Exception e) {
//			logger.error("", e);
//		} finally {
//			db.close();
//		}
//		return null;
//	}
	
	
	protected ArrayList<EncodingItem> getOutputs(String tbl, ArrayList<String> listCid) {
		ArrayList<EncodingItem> itemList = new ArrayList<>();

		DbTransaction db = new DbTransaction();
		Connection conn = null;

		try {
			conn = db.start();			
			String sql = null;			
			sql = "SELECT * FROM " + tbl + " WHERE cid=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {				
				for( String strCid : listCid ) {
					pstmt.setString(1, strCid);
					try( ResultSet rs = pstmt.executeQuery() ){
						if (rs.next()) {											
							EncodingItem item = new EncodingItem();
							item.cid = rs.getString("cid");
							item.sid = rs.getLong("sid");							
							item.copyright = rs.getString("copyright");							
							item.series = String.valueOf( rs.getInt("series") );
							item.extra_status = rs.getLong("extra_status");													
							item.nSubUsage = rs.getInt("sub_usage");
							item.framesize = rs.getString("framesize");							
							
							long lExtraStatus = rs.getLong("extra_status");
							if( ( lExtraStatus & ExtraStatusManipulation.Flag.EF_NEW_DRM.get() ) != 0 || 
								( lExtraStatus & ExtraStatusManipulation.Flag.EF_WATERMARK.get() ) != 0 ||
								( lExtraStatus & ExtraStatusManipulation.Flag.EF_ADD_ECDN_TASK.get() ) != 0 )
							{
								if( item.framesize.equals("UHD") || item.framesize.equals("UHD+HDR") ) {									
									item.strExt = "ts";
								} else {
									item.strExt = "mp4";
								}
							}else{
								item.strExt = "ts";
							}
							
							if( tbl.toLowerCase().equals("content") ){
								item.strType = rs.getString("type");
							}
							
							if( ( lExtraStatus & ExtraStatusManipulation.Flag.EF_WATERMARK.get() ) != 0 ) {
								item.bUseDummyEnc = true;
							}
							
							item.strRawTaskCaptionFileName = rs.getString("sub_raw_task_filename");
							
							item.capt_bas_lag_fg_cd = rs.getString("sub_base_lang");
							if( item.capt_bas_lag_fg_cd != null && !item.capt_bas_lag_fg_cd.isEmpty() ) {
								item.capt_bas_lag_fg_cd = DomaincodeConversion.ConverteLangEcdn( item.capt_bas_lag_fg_cd );
							}
							
							item.mtx_bas_lag_fg_cd = rs.getString("sub_lang_def");
							if( item.mtx_bas_lag_fg_cd != null && !item.mtx_bas_lag_fg_cd.isEmpty() ) {
								item.mtx_bas_lag_fg_cd = DomaincodeConversion.ConverteLangEcdn( item.mtx_bas_lag_fg_cd );
							}
							
							item.srcPath = rs.getString("1st_filepath");
							
							itemList.add(item);										
						}
								
					}					
				}
				
			}
			db.commit();
			return itemList;
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			db.close();
		}
		return null;
	}
	
	protected ArrayList<EncodingItem> getOutputs( ArrayList<Define.IDItem> listCid ) {
		ArrayList<EncodingItem> itemList = new ArrayList<>();

		DbTransaction db = new DbTransaction();
		Connection conn = null;

		try {
			conn = db.start();
			
			HashMap<String, String> mapOptSnQuality = new DbCommonHelper().GetMapDbDefineCodeOrderBy( conn, Define.MappingDefineCodeGrpCd.OPT_SN_QUALITY.toString(), true );
			
			//RTSP
			String sql = "SELECT * FROM cems.content WHERE cid=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {				
				for( Define.IDItem idItem : listCid ) {
					if( idItem.eIDType != Define.IDType.RTSP ) {
						continue;
					}
					
					pstmt.setString(1, idItem.strID);
					try( ResultSet rs = pstmt.executeQuery() ){
						if (rs.next()) {											
							EncodingItem item = new EncodingItem();
							item.nOptSnAssign = rs.getInt("encd_piqu_typ_cd");
							item.nOptSnQuality = rs.getInt("encd_piqu_lvl_cd");
							item.strEpadId = rs.getString("epsd_id");
							item.eIDType = Define.IDType.RTSP;
							item.cid = rs.getString("cid");
							item.sid = rs.getLong("sid");							
							item.copyright = rs.getString("copyright");
							item.extra_status = rs.getLong("extra_status");
							item.strPossnYn = rs.getString("possn_yn");
							if( rs.getInt("sub_usage") == 1 ) {
								item.bExistCaptionTask = true;
							}
							item.framesize = rs.getString("framesize");							
							
							long lExtraStatus = rs.getLong("extra_status");
							if( ( lExtraStatus & ExtraStatusManipulation.Flag.EF_NEW_DRM.get() ) != 0 || 
								( lExtraStatus & ExtraStatusManipulation.Flag.EF_WATERMARK.get() ) != 0 )								
							{
								if( item.framesize.equals("UHD") || item.framesize.equals("UHD+HDR") ) {									
									item.strExt = "ts";
								} else {
									item.strExt = "mp4";
								}
							}else{
								item.strExt = "ts";
							}
							
							item.strType = rs.getString("type");
							
							if( ( lExtraStatus & ExtraStatusManipulation.Flag.EF_WATERMARK.get() ) != 0 ) {
								item.bUseDummyEnc = true;
							}
							
							item.strRawTaskCaptionFileName = rs.getString("sub_raw_task_filename");							
							item.srcPath = rs.getString("1st_filepath");							
							item.outputFilename = replaceTagetFilename( item, mapOptSnQuality );
							
							ArrayList<Object[]> listAudioInfo = new ArrayList<>();							
							Object[] objAdoItem = new Object[2];
							objAdoItem[0] = rs.getInt("ado_encd_sort_seq");
							//objAdoItem[1] = rs.getString("ado_lag_fg_cd_iso639_2");
							listAudioInfo.add( objAdoItem );							
							item.listAudioInfo = listAudioInfo;
							
							if( rs.getInt("enc_opt_hdr_use") > 0 ) {
								item.lEncOptHdrId =  rs.getLong("enc_opt_hdr_id");								
							}
							
							itemList.add(item);										
						}
								
					}					
				}
				
			}
			
			//HLS
			sql = "SELECT * FROM cems.content_hls_enc WHERE cid=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {				
				for( Define.IDItem idItem : listCid ) {
					if( idItem.eIDType != Define.IDType.HLS ) {
						continue;
					}
					
					pstmt.setString(1, idItem.strID);
					try( ResultSet rs = pstmt.executeQuery() ){
						if (rs.next()) {							
							EncodingItem item = new EncodingItem();
							item.eIDType = Define.IDType.HLS;
							item.nOptSnAssign = rs.getInt("encd_piqu_typ_cd");
							item.nOptSnQuality = rs.getInt("encd_piqu_lvl_cd");
							item.strHlsEqualRtspCid = rs.getString( "equal_rtsp_cid" );
							item.strHlsMediaId = rs.getString("mda_id");
							item.cid = rs.getString("cid");
							item.sid = rs.getLong("sid");							
							item.copyright = rs.getString("copyright");
							item.extra_status = rs.getLong("extra_status");																				
							item.framesize = rs.getString("framesize");													
							if( rs.getString("framesize") != null && rs.getString("framesize").toUpperCase().contains("UHD") ) {								
								String strChkEcdnMp4UHDUse = new DbCommonHelper().GetConfigureDBValue(conn, "ecdn_use_uhd_mp4");
								if( strChkEcdnMp4UHDUse != null && strChkEcdnMp4UHDUse.equals("1") ) {
									item.strExt = "mp4";							
								}else {
									item.strExt = "ts";
								}
							}else {
								item.strExt = "mp4";							
							}							
							item.strType = rs.getString("type");								
							if( IsExistHlsCaptionInfo( conn, rs.getString("mda_id") ) ) {
								item.bHlsVttExtraction = true;								
								item.listHlsCaptLagFgCdCc = GetListHlsCaptionCdCc( conn, rs.getString("mda_id") );
							}
							
							if( rs.getInt("vtt_extraction") == 1 ) {
								item.bExistCaptionTask = true;
							}
							
							item.strRawTaskCaptionFileName = rs.getString("sub_raw_task_filename");							
							item.srcPath = rs.getString("1st_filepath");
							item.outputFilename = replaceTagetFilename( item, mapOptSnQuality );
							
							ArrayList<Object[]> listAudioInfo = new ArrayList<>();
							sql = "SELECT * FROM cems.content_hls_ado WHERE mda_id=? order by encd_sort_seq asc";
							try (PreparedStatement pstmt2 = conn.prepareStatement(sql)) {
								pstmt2.setString(1, rs.getString("mda_id") );
								logger.info( pstmt2.toString() );
								try( ResultSet rs2 = pstmt2.executeQuery() ){
									while( rs2.next() ){
										Object[] objAdoItem = new Object[2];
										objAdoItem[0] = rs2.getInt("encd_sort_seq");
										//objAdoItem[1] = rs2.getString("ado_lag_fg_cd_iso639_2");
										logger.info( String.format("HLS Audio Info | [ MediaID : %s ]", rs.getString("mda_id") ) );
										logger.info( String.format("HLS Audio Info | [ CID : %s ]", rs.getString("cid") ) );
										logger.info( String.format("HLS Audio Info | [ encd_sort_seq : %s ]", objAdoItem[0] ) );
										//logger.info( String.format("HLS Audio Info | [ ado_lag_fg_cd_iso639_2 : %s ]", objAdoItem[1] ) );
									
										listAudioInfo.add( objAdoItem );
									}
								}
							}
							item.listAudioInfo = listAudioInfo;
							logger.info( String.format("getOutput HLS Audio List Cnt : %d", item.listAudioInfo.size() ) );
							
							if( rs.getInt("enc_opt_hdr_use") > 0 ) {
								item.lEncOptHdrId =  rs.getLong("enc_opt_hdr_id");								
							}
							
							itemList.add(item);										
						}
								
					}					
				}
				
			}
			db.commit();
			return itemList;
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			db.close();
		}
		return null;
	}
	
	public static ArrayList<String> GetListHlsCaptionCdCc( Connection conn, String strHlsMediaId ) throws Exception
	{
		ArrayList<String> listRet = new ArrayList<>();
		
		try {
			if( strHlsMediaId == null || strHlsMediaId.isEmpty() ) {
				throw new Exception( "Param strHlsMediaId is Empty" );
			}
			
			String sql = "SELECT capt_lag_fg_cd_cc FROM cems.content_hls_capt WHERE mda_id=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {								
				pstmt.setString(1, strHlsMediaId);
				try( ResultSet rs = pstmt.executeQuery() ){
					while(rs.next()) {
						listRet.add( rs.getString(1) );												
						logger.info( String.format("Hls Caption Info | [ MediaID : %s ] [ Caption Cd CC : %s ]", strHlsMediaId, rs.getString(1) ) );
					}
				}				
			}
			
		}catch( Exception ex ) {
			logger.error("",ex);
			throw ex;
		}
		
		return listRet;
	}
	
	protected boolean IsExistHlsCaptionInfo( Connection conn, String strHlsMediaId ) throws Exception
	{
		boolean bRet = false;
		
		try {
			if( strHlsMediaId == null || strHlsMediaId.isEmpty() ) {
				throw new Exception( "Param strHlsMediaId is Empty" );
			}
			
			String sql = "SELECT count(*) FROM cems.content_hls_capt WHERE mda_id=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {								
				pstmt.setString(1, strHlsMediaId);
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {	
						if( rs.getInt(1) > 0 ) {
							bRet = true;							
						}				
						
						logger.info( String.format("Hls Caption Info Count | [ MediaID : %s ] [ Count : %d ]", strHlsMediaId, rs.getInt(1) ) );
					}
				}				
			}
			
		}catch( Exception ex ) {
			logger.error("",ex);
			throw ex;
		}
		
		return bRet;
	}
	
	protected String IsExistDummyEncTask( ArrayList<EncodingItem> outputs ) 
	{
		String strRet = "";
		
		try {
			do {
				if( outputs == null || outputs.size() <= 0 ) {
					logger.error( "EncodingItem Output Param is empty" );
					break;
				}
			
				boolean bUseDummyEnc = false;			
				
				for (EncodingItem output : outputs) {
					if( output.eIDType != Define.IDType.RTSP ) {
						continue;
					}
					
					if( output.bUseDummyEnc ) {
						bUseDummyEnc = true;
						logger.info( String.format( "Is Exsit Enc Use Dummy Enc Task !!! | [ CID : %s ]", output.cid ) );
						break;
					}					
				}
								
				if( bUseDummyEnc ) {
					strRet = new DbCommonHelper().GetConfigureDBValue(null, "dummy_preset_name");
					logger.info( String.format("Get Dummy Preset Name : %s", strRet ) );
				}
				
			}while( false );			
				
		}catch( Exception ex ) {
			logger.error("",ex);
		}
		
		return strRet;
	}
	
	protected ArrayList<String> GetDummyCidTypeWaterMark( ArrayList<EncodingItem> outputs, String strCidPerfix ) 
	{
		ArrayList<String> listRet = new ArrayList<>();
		
		try {
			do {
				if( outputs == null || outputs.size() <= 0 ) {
					logger.error( "Param EncodingItem Output is empty" );
					break;
				}
				
				if( strCidPerfix == null || strCidPerfix.isEmpty() ) {
					logger.error( "Param Cid Perfix is empty" );
					break;
				}
					
				ArrayList<String> listTempWaterMarkCids = new ArrayList<>();
				for (EncodingItem output : outputs) {
					if( output.eIDType != Define.IDType.RTSP ) {
						continue;
					}
					
					if( /*output.strType != null && output.strType.toUpperCase().equals("MOBILE") &&*/ ( output.extra_status & ExtraStatusManipulation.Flag.EF_WATERMARK.get() ) != 0 ) {
						listTempWaterMarkCids.add( output.cid );
					}					
				}	
				
				for( String strCid : listTempWaterMarkCids ) {
					if( strCid != null && strCid.contains( strCidPerfix ) ) {
						listRet.add( strCid );
					}
				}
				
			}while( false );			
				
		}catch( Exception ex ) {
			logger.error("",ex);
		}
		
		return listRet;
	}
	
	protected ArrayList<String> GetDummyCidTypeOri( ArrayList<EncodingItem> outputs ) 
	{
		ArrayList<String> listRet = new ArrayList<>();
		
		try {
			do {
				if( outputs == null || outputs.size() <= 0 ) {
					logger.error( "Param EncodingItem Output is empty" );
					break;
				}
												
				for (EncodingItem output : outputs) {										
					if( output.strType != null && output.strType.toUpperCase().equals("TV") ) {
						listRet.add( output.cid );
					}else {
						if( output.cid != null && output.cid.length() ==  output.cid.lastIndexOf("}") + 1 ) {
							listRet.add( output.cid );
						}	
					}
												
				}	
				
			}while( false );			
				
		}catch( Exception ex ) {
			logger.error("",ex);
		}
		
		return listRet;
	}
	
//	protected String replaceTagetFilename(String src, EncodingItem output) {
//		int ubar_start = src.indexOf("_");
//		int ubar_end = src.indexOf("_", ubar_start + 1);				
//		String inputChannelKeyword = src.substring(ubar_start, ubar_end) + "_";				
//		ubar_start = src.indexOf("_", ubar_end);
//		ubar_end = src.indexOf("_", ubar_start + 1);						
//		String inputNameKeyword = src.substring(ubar_start, ubar_end) + "_";
//		ubar_start = src.indexOf("_", ubar_end);
//		ubar_end = src.indexOf("_", ubar_start + 1);
//		String inputSeriesKeyword = src.substring(ubar_start, ubar_end) + "_";
//		
//		return src
//				.replace(inputChannelKeyword, "_" + output.channel + "_")
//				.replace(inputNameKeyword, "_" + MappingFilenameGen.replaceUnexpectedContentName(output.content_name) + "_")
//				.replace(inputSeriesKeyword, "_" + output.series + "_");	
//	}
	
	/*protected String replaceTagetFilename(String src, String targetCid) throws Exception {
		DbTransaction db = new DbTransaction();
		Connection conn = db.startAutoCommit();
		try (PreparedStatement pstmt = conn.prepareStatement("SELECT channel, series, content_name FROM content WHERE cid=?")) {
			pstmt.setString(1, targetCid);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				String channel = rs.getString("channel");
				String series = String.valueOf( rs.getInt("series") );
				String content_name = rs.getString("content_name");
				
				int ubar_start = src.indexOf("_");
				int ubar_end = src.indexOf("_", ubar_start + 1);				
				String inputChannelKeyword = src.substring(ubar_start, ubar_end) + "_";				
				ubar_start = src.indexOf("_", ubar_end);
				ubar_end = src.indexOf("_", ubar_start + 1);						
				String inputNameKeyword = src.substring(ubar_start, ubar_end) + "_";
				ubar_start = src.indexOf("_", ubar_end);
				ubar_end = src.indexOf("_", ubar_start + 1);
				String inputSeriesKeyword = src.substring(ubar_start, ubar_end) + "_";
				
				return src
						.replace(inputChannelKeyword, "_" + channel + "_")
						.replace(inputNameKeyword, "_" + MappingFilenameGen.replaceUnexpectedContentName(content_name) + "_")
						.replace(inputSeriesKeyword, "_" + series + "_");
			}
		} finally {
			db.close();
		}			
		
		return src;
	}*/
	
	protected String replaceTagetFilename(String src, String targetCid) throws Exception {
		DbTransaction db = new DbTransaction();
		Connection conn = db.startAutoCommit();
		try (PreparedStatement pstmt = conn.prepareStatement("SELECT possn_yn, channel, series, content_name FROM content WHERE cid=?")) {
			pstmt.setString(1, targetCid);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				String channel = rs.getString("channel");
				String series = String.valueOf( rs.getInt("series") );
				String content_name = rs.getString("content_name");
				String possn_yn = rs.getString("possn_yn");
				String strMarkPossnYN = "";
				if( possn_yn != null && possn_yn.toUpperCase().equals("Y") ){
					strMarkPossnYN = "_[소장용]";
				}
				
				int ubar_start = 0;
				int ubar_end = src.indexOf("_");				
				String inputChannelKeyword = src.substring(ubar_start, ubar_end) + "_";				
				ubar_start = src.indexOf("_", ubar_end);
				ubar_end = src.indexOf("_", ubar_start + 1);						
				String inputNameKeyword = src.substring(ubar_start, ubar_end) + "_";
				ubar_start = src.indexOf("_", ubar_end);
				ubar_end = src.indexOf("_", ubar_start + 1);
				String inputSeriesKeyword = src.substring(ubar_start, ubar_end) + "_";
				if(targetCid.contains("w0")) {
					return src
							.replace(inputChannelKeyword, channel + "_")
							.replace(inputNameKeyword, "_" + MappingFilenameGen.replaceUnexpectedContentName(content_name) + "_")
							.replace(inputSeriesKeyword, "_" + series + "_") + strMarkPossnYN + "_w0";
				}
				else if(targetCid.contains("w1")) {
					return src
							.replace(inputChannelKeyword, channel + "_")
							.replace(inputNameKeyword, "_" + MappingFilenameGen.replaceUnexpectedContentName(content_name) + "_")
							.replace(inputSeriesKeyword, "_" + series + "_") + strMarkPossnYN + "_w1";
				}else if(targetCid.contains("en") ){
					return src
							.replace(inputChannelKeyword, channel + "_")
							.replace(inputNameKeyword, "_" + MappingFilenameGen.replaceUnexpectedContentName(content_name) + "_")
							.replace(inputSeriesKeyword, "_" + series + "_") + strMarkPossnYN + "_ECDN";
				}else {
					return src
						.replace(inputChannelKeyword, channel + "_")
						.replace(inputNameKeyword, "_" + MappingFilenameGen.replaceUnexpectedContentName(content_name) + "_")
						.replace(inputSeriesKeyword, "_" + series + "_") + strMarkPossnYN;
				}
			}
		} finally {
			db.close();
		}			
		
		return src;
	}
		
	protected String replaceTagetFilename( EncodingItem encItem, HashMap<String, String> mapOptSnQuality ) throws Exception {
		String strRet = null;
		
		try {
			if( encItem == null ) {
				throw new Exception( "Param EncodingItem Empty" );
			}
			
			if( encItem.srcPath == null ) {
				throw new Exception( "Param EncodingItem Empty" );
			}
			
			String strPureFileName = Util.GetPureFileName( Paths.get( encItem.srcPath ).getFileName().toString() );
			
			if( strPureFileName == null || strPureFileName.isEmpty() ) {
				throw new Exception( String.format("Get Failue replaceTagetFilename Run, Pure Src File is Empty | [ IDType : %s ] [ CID : %s ] [ Src : %s ]", encItem.eIDType.toString(), encItem.cid, encItem.srcPath ) );
			}
			
			String strNamePart = strPureFileName.substring( 0, strPureFileName.lastIndexOf("_") );
			String strDataPart = strPureFileName.substring( strPureFileName.lastIndexOf("_"), strPureFileName.length() - 1 );
			
			if( encItem.eIDType == Define.IDType.RTSP ) {
				
				String strMarkPossnYN = "";
				if( encItem.strPossnYn != null && encItem.strPossnYn.toUpperCase().equals("Y") ) {
					strMarkPossnYN = "_[소장용]";
				}
				
				if( encItem.cid.contains("w0") ) {
					strRet = strNamePart + "_" + encItem.strEpadId + strDataPart + strMarkPossnYN + "_w0";
				}else if( encItem.cid.contains("w1") ) {
					strRet = strNamePart + "_" + encItem.strEpadId + strDataPart + strMarkPossnYN + "_w1";
				}else {
					strRet = strNamePart + "_" + encItem.strEpadId + strDataPart + strMarkPossnYN;
				}
				 				
			}else if( encItem.eIDType == Define.IDType.HLS ) {
				strRet = strNamePart + "_" + encItem.cid + strDataPart + "_HLS";
			}
			
			if( encItem.nOptSnAssign == Define.EncdPiquTypCd.Supernova.get() ) {
				
				String strQuality = Util.ConvNullIsEmptyStr( mapOptSnQuality.get( String.valueOf( encItem.nOptSnQuality ) ) ).toUpperCase();				
				String strType;
				if( encItem.strType.toUpperCase().equals("MOBILE") ) {
					strType = "MO";
				}else {
					strType = "TV";
				}
				
				strRet += ( String.format("_[SN-%s-%s-%s]", strType, encItem.framesize, strQuality ) );
			}
			
		}catch( Exception ex ) {
			logger.error("",ex);
			throw ex;
		}
		
		return strRet;		
	}
	
	protected String replaceTagetFilenameTv(String src, String targetCid) throws Exception {
		boolean isLegacyTv = Util.isLegacyTvCid(targetCid);
		DbTransaction db = new DbTransaction();
		Connection conn = db.startAutoCommit();
		try (PreparedStatement pstmt = conn.prepareStatement("SELECT possn_yn, channel, series, content_name FROM content WHERE cid=?")) {
			pstmt.setString(1, targetCid);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				String channel = rs.getString("channel");
				String series = String.valueOf( rs.getInt("series") );
				String content_name = rs.getString("content_name");
				String possn_yn = rs.getString("possn_yn");
				String strMarkPossnYN = "";
				if( possn_yn != null && possn_yn.toUpperCase().equals("Y") ){
					strMarkPossnYN = "_[소장용]";
				}
				
				int ubar_start = 0;
				int ubar_end = src.indexOf("_");				
				String inputChannelKeyword = src.substring(ubar_start, ubar_end) + "_";				
				ubar_start = src.indexOf("_", ubar_end);
				ubar_end = src.indexOf("_", ubar_start + 1);						
				String inputNameKeyword = src.substring(ubar_start, ubar_end) + "_";
				ubar_start = src.indexOf("_", ubar_end);
				ubar_end = src.indexOf("_", ubar_start + 1);
				String inputSeriesKeyword = src.substring(ubar_start, ubar_end) + "_";
				
				return src
						.replace(inputChannelKeyword, channel + "_")
						.replace(inputNameKeyword, "_" + MappingFilenameGen.replaceUnexpectedContentName(content_name) + "_")
						.replace(inputSeriesKeyword, "_" + series + "_") + strMarkPossnYN;				
			}
		} finally {
			db.close();
		}			
		
		return src;
	}
	
	protected String GetTvVttName(String src, String targetCid) throws Exception {
		boolean isLegacyTv = Util.isLegacyTvCid(targetCid);
		DbTransaction db = new DbTransaction();
		Connection conn = db.startAutoCommit();
		try (PreparedStatement pstmt = conn.prepareStatement("SELECT possn_yn, channel, series, content_name FROM content WHERE cid=?")) {
			pstmt.setString(1, targetCid);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				String channel = rs.getString("channel");
				String series = String.valueOf( rs.getInt("series") );
				String content_name = rs.getString("content_name");
				String possn_yn = rs.getString("possn_yn");
				String strMarkPossnYN = "";
				if( possn_yn != null && possn_yn.toUpperCase().equals("Y") ){
					strMarkPossnYN = "_[소장용]";
				}
				
				int ubar_start = 0;
				int ubar_end = src.indexOf("_");				
				String inputChannelKeyword = src.substring(ubar_start, ubar_end) + "_";				
				ubar_start = src.indexOf("_", ubar_end);
				ubar_end = src.indexOf("_", ubar_start + 1);						
				String inputNameKeyword = src.substring(ubar_start, ubar_end) + "_";
				ubar_start = src.indexOf("_", ubar_end);
				ubar_end = src.indexOf("_", ubar_start + 1);
				String inputSeriesKeyword = src.substring(ubar_start, ubar_end) + "_";
				if(isLegacyTv) {
					return src
							.replace(inputChannelKeyword, channel + "_")
							.replace(inputNameKeyword, "_" + MappingFilenameGen.replaceUnexpectedContentName(content_name) + "_")
							.replace(inputSeriesKeyword, "_" + series + "_") + strMarkPossnYN;
				} else {
					return src
							.replace(inputChannelKeyword, channel + "_")
							.replace(inputNameKeyword, "_" + MappingFilenameGen.replaceUnexpectedContentName(content_name) + "_")
							.replace(inputSeriesKeyword, "_" + series + "_") + strMarkPossnYN + "en";
				}
			}
		} finally {
			db.close();
		}			
		
		return src;
	}
	
//	protected void removeInputFile(String cid, SystemItem system, String inputpath, String calleeIp) throws Exception {
//		DbTransaction db = new DbTransaction();
//		Connection conn = db.startAutoCommit();
//		boolean remove = false;
//		String smi_cvt_filepath = null;
//		
//		try (PreparedStatement pstmt = conn.prepareStatement("SELECT set_num, type FROM content WHERE cid=?")) {
//			pstmt.setString(1, cid);
//			try( ResultSet rs = pstmt.executeQuery() ){
//				if (rs.next()) {
//					String setNum = rs.getString("set_num");
//					String type = rs.getString("type");
//					if (setNum != null && setNum.isEmpty() == false) { // SET 인 경우, 인코딩 중인 항목이 없으면 원본 파일 삭제
//						try (PreparedStatement pstmt2 = conn.prepareStatement(
//								"SELECT COUNT(*) FROM content WHERE set_num=? AND content_status='작업진행' AND status like '%%인코딩%%'")) {
//							pstmt2.setString(1, setNum);
//							try( ResultSet rs2 = pstmt2.executeQuery() ){
//								if (rs2.next()) {
//									int cnt = rs2.getInt(1);
//									logger.info("remained '작업진행'-'인코딩' = {}", cnt);
//									if (cnt == 0) { // 인코딩 상태의 작업진행 중인 SET 항목이 없으면 원본 삭제
//										remove = true;
//									}
//								}
//							}
//						}
//					} else if(type.toUpperCase().equals("TV")) {
//						try (PreparedStatement pstmt2 = conn.prepareStatement(
//								"SELECT COUNT(*) FROM content WHERE cid like ? AND UPPER(type)='TV' AND content_status='작업진행' AND status like '%%인코딩%%'")) {
//							pstmt2.setString(1, Util.pureMobileCid(cid) + '%');
//							try( ResultSet rs2 = pstmt2.executeQuery() ){
//								if (rs2.next()) {
//									int cnt = rs2.getInt(1);
//									logger.info("remained '작업진행'-'인코딩' = {}", cnt);
//									if (cnt == 0) { // 현재 스케줄과 관련된 인코딩 스케줄이 없으면 삭제.
//										remove = true;
//									}
//								}
//							}
//						}
//					} else if (type.toUpperCase().equals("MOBILE")) { // 모바일 인 경우
//						try (PreparedStatement pstmt2 = conn.prepareStatement(
//								"SELECT COUNT(*) FROM content WHERE cid like ? AND UPPER(type)='MOBILE' AND content_status='작업진행' AND status like '%%인코딩%%'")) {
//							pstmt2.setString(1, '%' + Util.mobileCid(cid));
//							try( ResultSet rs2 = pstmt2.executeQuery() ){
//								if (rs2.next()) {
//									int cnt = rs2.getInt(1);
//									logger.info("remained '작업진행'-'인코딩' = {}", cnt);
//									if (cnt == 0) { // 인코딩 상태의 작업진행 중인 모바일 항목이 없으면 원본 삭제
//										remove = true;
//									}
//								}
//							}
//						}
//					} else {
//						remove = true;
//						smi_cvt_filepath = rs.getString("sub_cvt_filepath");
//					}
//				}
//			}
//		} finally {
//			db.close();
//		}	
//		
//		if (remove) {
//			
//			try {
//				Path encOrigin = Paths.get(system.originPath);
//				String filename = Paths.get(inputpath).getFileName().toString();								
//				
//				//마지막 Input경로가 W0일지 W1일지 원본일지 모름
//				//따라서 순수원본Input 경로를 가지고 W0, W1, 원본Input 경로에 대해 삭제처리를 진행해야함				
//				
//				String strInputFileNamePure = filename.replace(".mov.W0.ts", "").replace(".mov.W1.ts", "");
//				String strExt = Util.GetExt( strInputFileNamePure ); 
//				strInputFileNamePure = Util.GetPureFileName( strInputFileNamePure );
//				
//				logger.info( String.format( "Remove Enc Input FileName Pure : %s", strInputFileNamePure ) );				
//				
//				String delpath = "\\\\" + system.ip + encOrigin.resolve(strInputFileNamePure + strExt).toString().replace('/', '\\');
//				logger.info( String.format("EncInput Src File Delete Start !!!!! | [ %s ]", delpath ) );				
//				EMSProcServiceDeleteFileIo delIo = new EMSProcServiceDeleteFileIo(delpath, null);
//				delIo.setSrcIp(calleeIp);
//				delIo.start(null);
//							
//				delpath = "\\\\" + system.ip + encOrigin.resolve(strInputFileNamePure + strExt + ".mov").toString().replace('/', '\\');
//				logger.info( String.format("Wartermark Dummy Mov File Delete | [ %s ]", delpath) );
//				delIo = null;
//				delIo = new EMSProcServiceDeleteFileIo(delpath, null);
//				delIo.setSrcIp(calleeIp);
//				delIo.start(null);
//				
//				delpath = "\\\\" + system.ip + encOrigin.resolve(strInputFileNamePure + strExt + ".mov.W0.ts").toString().replace('/', '\\');
//				logger.info( String.format("Wartermark Dummy W0 File Delete | [ %s ]", delpath) );
//				delIo = null;
//				delIo = new EMSProcServiceDeleteFileIo(delpath, null);
//				delIo.setSrcIp(calleeIp);
//				delIo.start(null);
//							
//				delpath = "\\\\" + system.ip + encOrigin.resolve(strInputFileNamePure + strExt + ".mov.W1.ts").toString().replace('/', '\\');
//				logger.info( String.format("Wartermark Dummy W1 File Delete | [ %s ]", delpath) );
//				delIo = null;
//				delIo = new EMSProcServiceDeleteFileIo(delpath, null);
//				delIo.setSrcIp(calleeIp);
//				delIo.start(null);
//				
//				if( smi_cvt_filepath != null && !smi_cvt_filepath.isEmpty() ){
//					//SMI Remove
//					filename = Paths.get(smi_cvt_filepath).getFileName().toString();
//					delpath = "\\\\" + system.ip + encOrigin.resolve(filename).toString().replace('/', '\\');
//					delIo = null;
//					delIo = new EMSProcServiceDeleteFileIo(delpath, null);
//					delIo.setSrcIp(calleeIp);
//					delIo.start(null);
//				}
//			}catch( Exception ex ) {
//				logger.error("",ex);
//			}
//			
//		}
//	}
	
	
	protected void removeInputFileEx(String cid, SystemItem system, String inputpath, String calleeIp) throws Exception {
		DbTransaction db = new DbTransaction();
		Connection conn = db.startAutoCommit();
		boolean remove = false;
		String smi_cvt_filepath = null;
		
		
		
		if( Util.IdTypeIsRTSP( cid ) ) {
			// RTSP
			try (PreparedStatement pstmt = conn.prepareStatement("SELECT sub_cvt_filepath, framesize, epsd_id, possn_yn, set_num, type FROM content WHERE cid=? and sid=?")) {
				pstmt.setString(1, cid);
				pstmt.setLong(2, system.id);
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {
						String strFramesize = rs.getString("framesize");
						String strEpsdId = rs.getString("epsd_id");
						String strPossnYn = rs.getString("possn_yn");
						String setNum = rs.getString("set_num");
						String type = rs.getString("type");
						smi_cvt_filepath = rs.getString("sub_cvt_filepath");
						
						if (setNum != null && setNum.isEmpty() == false) { // SET 인 경우, 인코딩 중인 항목이 없으면 원본 파일 삭제
							
							int cnt = 0;
							
							//RTSP
							try (PreparedStatement pstmt2 = conn.prepareStatement(
									"SELECT COUNT(*) FROM content WHERE set_num=? AND content_status='작업진행' AND status like '[원본]인코딩(%%' AND sid=?")) {
								pstmt2.setString(1, setNum);
								pstmt2.setLong(2, system.id);
								logger.info( pstmt2.toString() );
								try( ResultSet rs2 = pstmt2.executeQuery() ){
									if (rs2.next()) {
										cnt += rs2.getInt(1);
										logger.info("remained '작업진행'-'인코딩' = {}", cnt);
										
									}
								}
							}
							
							// HLS
							try (PreparedStatement pstmt2 = conn.prepareStatement(
									"SELECT COUNT(*) FROM content_hls_enc WHERE set_num=? AND status like '[원본]인코딩(%%' AND sid=?")) {
								pstmt2.setString(1, setNum);
								pstmt2.setLong(2, system.id);
								logger.info( pstmt2.toString() );
								try( ResultSet rs2 = pstmt2.executeQuery() ){
									if (rs2.next()) {
										cnt += rs2.getInt(1);
										logger.info("remained '작업진행'-'인코딩' = {}", cnt);										
									}
								}
							}
							
							if (cnt == 0) { // 인코딩 상태의 작업진행 중인 SET 항목이 없으면 원본 삭제
								remove = true;
							}
							
						} else if(type.toUpperCase().equals("TV")) {
							try (PreparedStatement pstmt2 = conn.prepareStatement(
									"SELECT COUNT(*) FROM content WHERE cid=? AND UPPER(type)='TV' AND content_status='작업진행' AND status like '[원본]인코딩(%%' and sid=?")) {
								pstmt2.setString(1, cid );
								pstmt2.setLong(2, system.id);
								logger.info( pstmt2.toString() );
								try( ResultSet rs2 = pstmt2.executeQuery() ){
									if (rs2.next()) {
										int cnt = rs2.getInt(1);
										logger.info("remained '작업진행'-'인코딩' = {}", cnt);
										if (cnt == 0) { // 현재 스케줄과 관련된 인코딩 스케줄이 없으면 삭제.
											remove = true;
										}
									}
								}
							}
						} else if (type.toUpperCase().equals("MOBILE")) { // 모바일 인 경우
							
							if( strFramesize != null && strFramesize.toUpperCase().equals("FHD") ) {
								
								try (PreparedStatement pstmt2 = conn.prepareStatement(
										"SELECT COUNT(*) FROM content WHERE possn_yn=? and epsd_id=? and framesize='FHD' and UPPER(type)='MOBILE' AND content_status='작업진행' AND status like '[원본]인코딩(%%' and sid=?")) {
									int args = 1;
									pstmt2.setString(args++, strPossnYn );
									pstmt2.setString(args++, strEpsdId );									
									pstmt2.setLong(args++, system.id);
									logger.info( pstmt2.toString() );
									try( ResultSet rs2 = pstmt2.executeQuery() ){
										if (rs2.next()) {
											int cnt = rs2.getInt(1);
											logger.info("remained '작업진행'-'인코딩' = {}", cnt);
											if (cnt == 0) { // 인코딩 상태의 작업진행 중인 모바일 항목이 없으면 원본 삭제
												remove = true;
											}
										}
									}
								}
								
							}else {
								try (PreparedStatement pstmt2 = conn.prepareStatement(
										"SELECT COUNT(*) FROM content WHERE possn_yn=? and epsd_id=? and framesize!='FHD' and UPPER(type)='MOBILE' AND content_status='작업진행' AND status like '[원본]인코딩(%%' and sid=?")) {
									int args = 1;
									pstmt2.setString(args++, strPossnYn );
									pstmt2.setString(args++, strEpsdId );
									pstmt2.setLong(args++, system.id);
									logger.info( pstmt2.toString() );
									try( ResultSet rs2 = pstmt2.executeQuery() ){
										if (rs2.next()) {
											int cnt = rs2.getInt(1);
											logger.info("remained '작업진행'-'인코딩' = {}", cnt);
											if (cnt == 0) { // 인코딩 상태의 작업진행 중인 모바일 항목이 없으면 원본 삭제
												remove = true;
											}
										}
									}
								}	
							}
							
						} else {
							remove = true;							
						}
					}
				}
			} finally {
				db.close();
			}
		}else {
			// HLS
			
			try (PreparedStatement pstmt = conn.prepareStatement("SELECT sub_cvt_filepath, mda_id, set_num, type FROM content_hls_enc WHERE cid=?")) {
				pstmt.setString(1, cid);
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {
						smi_cvt_filepath = rs.getString("sub_cvt_filepath");
						String strMediaID = rs.getString("mda_id");
						String strSetNum = rs.getString("set_num");
						if( strSetNum != null && !strSetNum.isEmpty() ) {
							
							int cnt = 0;
							// HLS
							try (PreparedStatement pstmt2 = conn.prepareStatement( "SELECT COUNT(*) FROM content_hls_enc WHERE set_num=? and status like '[원본]인코딩(%%'" )) {
								int args = 1;
								pstmt2.setString(args++, strSetNum );								
								logger.info( pstmt2.toString() );
								try( ResultSet rs2 = pstmt2.executeQuery() ){
									if (rs2.next()) {
										cnt += rs2.getInt(1);
										logger.info("remained '작업진행'-'인코딩' = {}", cnt);										
									}
								}
							}
							
							// RTSP
							try (PreparedStatement pstmt2 = conn.prepareStatement( "SELECT COUNT(*) FROM content WHERE set_num=? and status like '[원본]인코딩(%%'" )) {
								int args = 1;
								pstmt2.setString(args++, strSetNum );								
								logger.info( pstmt2.toString() );
								try( ResultSet rs2 = pstmt2.executeQuery() ){
									if (rs2.next()) {
										cnt += rs2.getInt(1);
										logger.info("remained '작업진행'-'인코딩' = {}", cnt);										
									}
								}
							}							
							
							if (cnt == 0) { // 인코딩 상태의 작업진행 중인 모바일 항목이 없으면 원본 삭제
								remove = true;
							}
							
						}else{
							
							try (PreparedStatement pstmt2 = conn.prepareStatement( "SELECT COUNT(*) FROM content_hls_enc WHERE mda_id=? and status like '[원본]인코딩(%%'" )) {
								int args = 1;
								pstmt2.setString(args++, strMediaID );								
								logger.info( pstmt2.toString() );
								try( ResultSet rs2 = pstmt2.executeQuery() ){
									if (rs2.next()) {
										int cnt = rs2.getInt(1);
										logger.info("remained '작업진행'-'인코딩' = {}", cnt);
										if (cnt == 0) { // 인코딩 상태의 작업진행 중인 모바일 항목이 없으면 원본 삭제
											remove = true;
										}
									}
								}
							}							
						}
					}
				}			
			}
			
		}
		
		if (remove) {
			
			try {
				Path encOrigin = Paths.get(system.originPath);
				String filename = Paths.get(inputpath).getFileName().toString();								
				
				//마지막 Input경로가 W0일지 W1일지 원본일지 모름
				//따라서 순수원본Input 경로를 가지고 W0, W1, 원본Input 경로에 대해 삭제처리를 진행해야함				
				
				String strInputFileNamePure = filename.replace(".mov.W0.ts", "").replace(".mov.W1.ts", "");
				String strExt = Util.GetExt( strInputFileNamePure ); 
				strInputFileNamePure = Util.GetPureFileName( strInputFileNamePure );
				
				logger.info( String.format( "Remove Enc Input FileName Pure : %s", strInputFileNamePure ) );				
				
				String delpath = "\\\\" + system.ip + encOrigin.resolve(strInputFileNamePure + strExt).toString().replace('/', '\\');
				logger.info( String.format("EncInput Src File Delete Start !!!!! | [ %s ]", delpath ) );				
				EMSProcServiceDeleteFileIo delIo = new EMSProcServiceDeleteFileIo(delpath, null);
				delIo.setSrcIp(calleeIp);
				delIo.start(null);
							
				delpath = "\\\\" + system.ip + encOrigin.resolve(strInputFileNamePure + strExt + ".mov").toString().replace('/', '\\');
				logger.info( String.format("Wartermark Dummy Mov File Delete | [ %s ]", delpath) );
				delIo = null;
				delIo = new EMSProcServiceDeleteFileIo(delpath, null);
				delIo.setSrcIp(calleeIp);
				delIo.start(null);
				
				delpath = "\\\\" + system.ip + encOrigin.resolve(strInputFileNamePure + strExt + ".mov.W0.ts").toString().replace('/', '\\');
				logger.info( String.format("Wartermark Dummy W0 File Delete | [ %s ]", delpath) );
				delIo = null;
				delIo = new EMSProcServiceDeleteFileIo(delpath, null);
				delIo.setSrcIp(calleeIp);
				delIo.start(null);
							
				delpath = "\\\\" + system.ip + encOrigin.resolve(strInputFileNamePure + strExt + ".mov.W1.ts").toString().replace('/', '\\');
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
			}catch( Exception ex ) {
				logger.error("",ex);
			}
			
		}
	}
	
	protected void copyRemoteFile(String from, String to, String servletQ, String cid) throws Exception {		
		EMSProcServiceCopyFileIo cp = new EMSProcServiceCopyFileIo(from.replace("/", "\\"), to.replace("/", "\\"));
		cp.setServletParam(servletQ);
		if (cp.start(null)) {
			logger.info("file i/o success / {} -> {}", from, to);
		} else {
			logger.info("file i/o failure / {} -> {}", from, to);
			throw cp.getException();
		}
	}	
	
	public static void copyFile(String from, String to) throws Exception {
		FileIo cp = new CopyFileIo(from, to);
		if (cp.start(null)) {
			logger.info("file i/o success / {} -> {}", from, to);
		} else {
			logger.info("file i/o failure / {} -> {}", from, to);
			throw cp.getException();
		}
	}
	
	
	protected boolean copyFiles(HashMap<CopyKey, ArrayList<Path>> copyPaths) {
		int success = 0;		
		for (Entry<CopyKey, ArrayList<Path>> entry : copyPaths.entrySet()) {
			String from = entry.getValue().get(0).toString();
			String to = entry.getValue().get(1).toString();
			CopyFileIo cp = new CopyFileIo(from, to);
			if (cp.start(null)) {
				logger.info("[" + entry.getKey() + "] file i/o success / from=" + from + ", to=" + to);
				success++;
			} else {
				logger.info("[" + entry.getKey() + "] file i/o failure / from=" + from + ", to=" + to);
				break;
			}
		}
		
		return copyPaths.size() == success;
	}
	
	@SuppressWarnings("unchecked")
	public static void abortEncodingJob(String eamsHost, String cid) throws Exception {
			String apiUrl = EmsPropertiesReader.get("eams.api.encodingstop").replace("*", eamsHost);
			HttpRESTful restful = HttpRESTful.open("POST", apiUrl);
			restful.setHeader("Content-Type","application/json; charset=UTF-8");
			JSONObject json = new JSONObject();
			JSONArray jsonArray = new JSONArray();
			JSONObject jsonArrayItem = new JSONObject();
			jsonArrayItem.put("cid", cid);
			jsonArray.add(jsonArrayItem);
			json.put("output_list", jsonArray);
			restful.setJson(json);
			restful.connect();
	}
	
	@SuppressWarnings("unchecked")
	public static void abortEncodingJob(String eamsHost, ArrayList<String> listCids ) throws Exception {
			String apiUrl = EmsPropertiesReader.get("eams.api.encodingstop").replace("*", eamsHost);
			HttpRESTful restful = HttpRESTful.open("POST", apiUrl);
			restful.setHeader("Content-Type","application/json; charset=UTF-8");
			JSONObject json = new JSONObject();
			JSONArray jsonArray = new JSONArray();
			for( String strCid : listCids ) {
				JSONObject jsonArrayItem = new JSONObject();			
				jsonArrayItem.put("cid", strCid);
				jsonArray.add(jsonArrayItem);
			}			
			json.put("output_list", jsonArray);
			restful.setJson(json);
			restful.connect();
	}
	
	
		
	protected void updateDownloadStatus(String tbl, ArrayList<EncodingItem> outputs, long sid) throws Exception {
		DbTransaction db = new DbTransaction();
		Connection conn = null;

		try {
			conn = db.start();
			String sql = "UPDATE " + tbl + " SET last_time=now(3), content_status='작업진행', status='[원본]다운로드', extra_status=(extra_status & ~?), sid=?  WHERE cid=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				for (EncodingItem item : outputs) {
					pstmt.setInt(1, ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get());
					pstmt.setLong(2, sid);
					pstmt.setString(3, item.cid);
					logger.info(pstmt.toString());
					pstmt.executeUpdate();
					if (tbl.equals("clip")) {
						DbHelper.insertClipHistroy(conn, "cid='" + item.cid + "'");
					} else {
						DbHelper.insertContentHistroy(conn, "cid='" + item.cid + "'");
					}
				}
			}
						
			db.commit();
			if (tbl.equals("clip")) {
				ClipTaskManager.signal();
			} else {
				TranscodingTask.signal();
				TranscodingStatus.signal();
				BroadcastTaskManager.signal();
			}
		} catch (Exception e) {
			db.rollback();
			throw e;
		} finally {
			db.close();
		}
	}
	
	
	protected void updateDownloadStatusEx( ArrayList<EncodingItem> outputs, long sid) throws Exception {
		DbTransaction db = new DbTransaction();
		Connection conn = null;

		try {
			conn = db.start();
			String sql = "UPDATE content SET last_time=now(3), content_status='작업진행', status='[원본]다운로드', extra_status=(extra_status & ~?), sid=?, mark_enc_complete=null, enc_output_path=null, enc_real_output_path=null  WHERE cid=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				for (EncodingItem item : outputs) {
					if( item.eIDType != Define.IDType.RTSP ) {
						continue;
					}
					int args = 1;
					pstmt.setInt(args++, ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get());
					pstmt.setLong(args++, sid);
					pstmt.setString(args++, item.cid);
					logger.info(pstmt.toString());
					pstmt.executeUpdate();					
					DbHelper.insertContentHistroy(conn, "cid='" + item.cid + "'");					
				}
			}
			
			sql = "UPDATE content_hls_enc a left outer join content_hls b on a.mda_id = b.mda_id SET "
					+ "a.last_time=now(3), a.status='[원본]다운로드', a.extra_status=(extra_status & ~?), a.sid=?, b.mda_matl_sts_cd='작업진행' "
					+ "WHERE a.cid=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				for (EncodingItem item : outputs) {
					if( item.eIDType != Define.IDType.HLS ) {
						continue;
					}
					int args = 1;
					pstmt.setInt(args++, ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get());
					pstmt.setLong(args++, sid);					
					pstmt.setString(args++, item.cid);
					logger.info(pstmt.toString());
					pstmt.executeUpdate();	
					DbHelper.insertContentHistroyHls(conn, item.cid);
				}
			}
						
			db.commit();
			
		} catch (Exception e) {
			db.rollback();
			throw e;
		} finally {
			db.close();
			TranscodingTask.signal();
			TranscodingStatus.signal();
			BroadcastTaskManager.signal();
		}
	}
	
	protected void updateDownloadStatus(String tbl, ArrayList<EncodingItem> outputs) throws Exception {
		DbTransaction db = new DbTransaction();
		Connection conn = null;

		try {
			conn = db.start();
			String sql = "UPDATE " + tbl + " SET last_time=now(3), content_status='작업진행', status='[원본]다운로드', extra_status=(extra_status & ~?), sid=?  WHERE cid=?";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				for (EncodingItem item : outputs) {
					pstmt.setInt(1, ExtraStatusManipulation.Flag.EF_RESERVED_JOB.get());
					pstmt.setLong(2, item.sid);
					pstmt.setString(3, item.cid);
					logger.info(pstmt.toString());
					pstmt.executeUpdate();
					if (tbl.equals("clip")) {
						DbHelper.insertClipHistroy(conn, "cid='" + item.cid + "'");
					} else {
						DbHelper.insertContentHistroy(conn, "cid='" + item.cid + "'");
					}
				}
			}
						
			db.commit();
			if (tbl.equals("clip")) {
				ClipTaskManager.signal();
			} else {
				TranscodingTask.signal();
				TranscodingStatus.signal();
				BroadcastTaskManager.signal();
			}
		} catch (Exception e) {
			db.rollback();
			throw e;
		} finally {
			db.close();
		}
	}
	
	protected void updateEncodingStatus(String tbl, String contentStatus, String status, SystemItem system, ArrayList<EncodingItem> outputs, String note) throws Exception {
		DbTransaction db = new DbTransaction();
		Connection conn = null;

		try {
			conn = db.start();
			try (PreparedStatement pstmt = conn.prepareStatement("UPDATE " + tbl + " SET last_time=now(3), sid=?, content_status=?, status=?, note=? WHERE cid=?")) {
				for (EncodingItem item : outputs) {
					pstmt.setLong(1, system.id);
					pstmt.setString(2, contentStatus);
					pstmt.setString(3, status);
					pstmt.setString(4, Util.CuttingMaximumStr( note, 500 ) );
					pstmt.setString(5, item.cid);
					logger.info(pstmt.toString());
					pstmt.executeUpdate();
					if (tbl.equals("clip")) {
						DbHelper.insertClipHistroy(conn, "cid='" + item.cid + "'");
					} else {
						DbHelper.insertContentHistroy(conn, "cid='" + item.cid + "'");
					}
				}
			}
			db.commit();
			
			if (tbl.equals("clip")) {
				ClipTaskManager.signal();
			} else {
				TranscodingStatus.signal();
				TranscodingTask.signal();
				BroadcastTaskManager.signal();
			}
		} catch (Exception e) {
			db.rollback();
			throw e;
		} finally {
			db.close();
		}
	}
	
	protected void updateEncodingStatusEx( String contentStatus, String status, SystemItem system, ArrayList<EncodingItem> outputs, String note) throws Exception {
		DbTransaction db = new DbTransaction();
		Connection conn = null;

		try {
			conn = db.start();
			try (PreparedStatement pstmt = conn.prepareStatement("UPDATE content SET last_time=now(3), sid=?, content_status=?, status=?, note=? WHERE cid=?")) {
				for (EncodingItem item : outputs) {
					if( item.eIDType != Define.IDType.RTSP ) {
						continue;
					}
					int args = 1;
					pstmt.setLong(args++, system.id);
					pstmt.setString(args++, contentStatus);
					pstmt.setString(args++, status);
					pstmt.setString(args++, Util.CuttingMaximumStr( note, 500 ) );
					pstmt.setString(args++, item.cid);
					logger.info(pstmt.toString());
					pstmt.executeUpdate();					
					DbHelper.insertContentHistroy(conn, "cid='" + item.cid + "'");					
				}
			}
			
			try (PreparedStatement pstmt = conn.prepareStatement("UPDATE content_hls_enc a left outer join content_hls b on a.mda_id = b.mda_id SET a.last_time=now(3), a.sid=?, b.mda_matl_sts_cd=?, a.status=?, a.note=? WHERE a.cid=?")) {
				for (EncodingItem item : outputs) {
					if( item.eIDType != Define.IDType.HLS ) {
						continue;
					}
					int args = 1;
					pstmt.setLong(args++, system.id);
					pstmt.setString(args++, contentStatus);
					pstmt.setString(args++, status);
					pstmt.setString(args++, Util.CuttingMaximumStr( note, 500 ));
					pstmt.setString(args++, item.cid);
					logger.info(pstmt.toString());
					pstmt.executeUpdate();		
					DbHelper.insertContentHistroyHls(conn, item.cid);
				}
			}
			db.commit();
						
		} catch (Exception e) {
			db.rollback();
			throw e;
		} finally {
			db.close();
			TranscodingStatus.signal();
			TranscodingTask.signal();
			BroadcastTaskManager.signal();
		}
	}
	
	public static JSONObject requestSubtitleConvert(String smi, String caution, String copyright, String cvt) throws Exception {
		String eamsIp = null;
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		conn = db.startAutoCommit();
		try (PreparedStatement pstmt = conn.prepareStatement("SELECT value FROM configure WHERE configure.key='eams_host'")) {
			try( ResultSet rs = pstmt.executeQuery() ){
				if (rs.next()) {
					eamsIp = rs.getString(1);
				}
			}
		} finally {
			db.close();
		}
		
		String apiUrl = EmsPropertiesReader.get("eams.api.subtitle_conv").replace("*", eamsIp);
		
		if (smi == null || smi.isEmpty()) {
			smi = caution;
		}
		
		for (int i = 0; i < 3; i++) {
			try {
				HttpRESTful restful = HttpRESTful.open("POST", apiUrl);
				restful.setReadTimeout(10000);
				restful.setHeader("Content-Type", "application/json; charset=UTF-8");

				JSONObject json = new JSONObject();
				json.put("smiPath", Paths.get(smi).getFileName().toString());
				json.put("cvtSmiPath", cvt);

				if (copyright != null && copyright.isEmpty() == false) {
					json.put("copyright", "1");
					if (caution != null && caution.isEmpty() == false) {
						json.put("cautionPath", Paths.get(caution).getFileName().toString());
						json.replace("copyright", "2");
					}
				}
				
				restful.setJson(json);
				JSONObject response = restful.connect();
				if (response == null) {
					continue;
				}
				return response;
			} catch (Exception e) {
				logger.error("", e);
			}	
		}

		throw new EmsException(ErrorCode.HTTP_OPEN_ERROR, apiUrl + " API 호출실패");
	}
	
	/*자막파일을 컨버팅하도록 Http로 EAMS에게 요청한다.(VTT)*/
	public static JSONObject requestSubtitleConvertVtt(String smi, String vtt) throws Exception {
		String eamsIp = null;
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		conn = db.startAutoCommit();
		try (PreparedStatement pstmt = conn.prepareStatement("SELECT value FROM configure WHERE configure.key='eams_host'")) {
			try( ResultSet rs = pstmt.executeQuery() ){
				if (rs.next()) {
					eamsIp = rs.getString(1);
				}
			}
		} finally {
			db.close();
		}
		
		String apiUrl = EmsPropertiesReader.get("eams.api.subtitle_convVtt").replace("*", eamsIp);
		
		for (int i = 0; i < 3; i++) {
			try {
				HttpRESTful restful = HttpRESTful.open("POST", apiUrl);
				restful.setReadTimeout(10000);
				restful.setHeader("Content-Type", "application/json; charset=UTF-8");

				JSONObject json = new JSONObject();
				json.put("smiPath", Paths.get(smi).getFileName().toString());
				json.put("cvtSmiPath", vtt);
								
				restful.setJson(json);
				JSONObject response = restful.connect();
				if (response == null) {
					continue;
				}
				return response;
			} catch (Exception e) {
				logger.error("", e);
			}	
		}

		throw new EmsException(ErrorCode.HTTP_OPEN_ERROR, apiUrl + " API 호출실패");
	}
	
	protected JSONObject GetJsonEncOpt( long lEncOptId ) throws Exception
	{
		JSONObject jsRet = null;

		DbTransaction db = new DbTransaction();
		Connection conn = null;
		String sql = null;
		
		try {			
			if( lEncOptId < 0 ) {
				throw new Exception( String.format("Param Enc Opt Id is UnNormal Value | [ %d ]", lEncOptId ) );
			}

			db = new DbTransaction();
			conn = db.startAutoCommit();
			try {			
				sql = "select * from cems.enc_opt where id=?";
				try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
					pstmt.setLong(1, lEncOptId);
					logger.debug(pstmt.toString());
					try( ResultSet rs = pstmt.executeQuery() ){
						if (rs.next()) {
							String strCommEncOptCd = rs.getString( "comm_opt_cd" );
							if( strCommEncOptCd == null || strCommEncOptCd.isEmpty() ) {
								throw new Exception( String.format("Enc Opt Common Opt Cd is Empty | [ ID : %d ]", lEncOptId ) );
							}
							
							if( strCommEncOptCd.toUpperCase().equals( Define.EncOptCd.OPT_HDR.toString() ) ) {
								jsRet = GetJsonHDR( rs );
							}else {
								throw new Exception( String.format("Enc Opt Common Opt Cd is Unknown Type | [ ID : %d ] [ %s ]", lEncOptId, strCommEncOptCd ) );
							}
						}		
					}
				}
			} catch (Exception e) {
				logger.error("", e);			
			} finally {
				db.close();
			}				
						
		}catch( Exception ex ) {
			logger.error("",ex);
			throw ex;
		}
		
		return jsRet;
	}
	
	@SuppressWarnings("unchecked")
	protected JSONObject GetJsonHDR( ResultSet rs ) throws Exception
	{
		JSONObject jsRet = null;
		
		try {			
			if( rs == null ) {
				throw new Exception( "Param ResultSet is Empty" );
			}

			JSONObject jsTemp = new JSONObject();
			jsTemp.put( "color_space", rs.getString("color_space") );
			jsTemp.put( "force_color", rs.getInt("force_color") > 0 ? true : false );
			jsTemp.put( "filter_enable", rs.getString("filter_enable") );
			jsTemp.put( "no_psi", rs.getInt("no_psi") > 0 ? true : false );
			jsTemp.put( "timecode_source", rs.getString("timecode_source") );
			jsTemp.put( "deblock_selected", rs.getInt("deblock_selected") > 0 ? true : false );
			jsTemp.put( "denoise_selected", rs.getInt("denoise_selected") > 0 ? true : false );
			jsTemp.put( "filter_strength", rs.getInt("filter_strength") );
			jsTemp.put( "pid", Util.ConvNullIsEmptyStr( rs.getString("pid") ) );
			jsTemp.put( "program_id", Util.ConvNullIsEmptyStr( rs.getString("program_id") ) );
			
			jsTemp.put( "red_primary_x", rs.getInt("red_primary_x") );
			jsTemp.put( "red_primary_y", rs.getInt("red_primary_y") );
			jsTemp.put( "green_primary_x", rs.getInt("green_primary_x") );
			jsTemp.put( "green_primary_y", rs.getInt("green_primary_y") );
			jsTemp.put( "blue_primary_x", rs.getInt("blue_primary_x") );
			jsTemp.put( "blue_primary_y", rs.getInt("blue_primary_y") );
			jsTemp.put( "white_point_x", rs.getInt("white_point_x") );
			jsTemp.put( "white_point_y", rs.getInt("white_point_y") );
			jsTemp.put( "max_luminance", rs.getInt("max_luminance") );
			jsTemp.put( "min_luminance", rs.getInt("min_luminance") );
			jsTemp.put( "maxcll", rs.getInt("maxcll") );
			jsTemp.put( "maxfall", rs.getInt("maxfall") );			
			
			jsRet = jsTemp;
			
		}catch( Exception ex ) {
			logger.error("",ex);
			throw ex;
		}
		
		return jsRet;
	}
	
	
	@SuppressWarnings("unchecked")
	protected JSONObject requestEncoding(String cid, SystemItem encoder, Path input, ArrayList<EncodingItem> outputs, Boolean isContent ) throws Exception {
		String eamsIp = null;
		String strReqIP = null;
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		conn = db.startAutoCommit();
		
		try {
			try (PreparedStatement pstmt = conn.prepareStatement("SELECT value FROM configure WHERE configure.key='eams_host'")) {
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {
						eamsIp = rs.getString(1);
					}
				}
			}	
			
			//strReqIP = new TaskReqReplaceIP().ChkIP(conn, encoder.ip);
			strReqIP = new ConvIP().excute(conn, ConvIPFlag.elemental, ConvIPReplaceFlag.c_class, encoder.ip);
			
			if( strReqIP == null || strReqIP.isEmpty() ) {
				throw new Exception( String.format( "1G Elemental Request IP Get Failure | [ %s ]", encoder.ip  ) );
			}
		}catch( Exception ex ) {
			logger.error("",ex);
			throw ex;
		}finally {
			db.close();
		}		
		
		String apiUrl = EmsPropertiesReader.get("eams.api.encoding").replace("*", eamsIp);
		
		//워터마크 작업 설정
		ArrayList< String > listDummyCidOri = new ArrayList<>();
		ArrayList< String > listDummyCidType0 = new ArrayList<>();
		ArrayList< String > listDummyCidType1 = new ArrayList<>();
		
		String strDummyPreset = IsExistDummyEncTask( outputs );
		if( strDummyPreset != null && !strDummyPreset.isEmpty() ) {
			listDummyCidOri = GetDummyCidTypeOri( outputs );
			listDummyCidType0 = GetDummyCidTypeWaterMark(outputs, "w0e" );
			listDummyCidType1 = GetDummyCidTypeWaterMark(outputs, "w1e" );
		}
		
				
		for (int i = 0; i < 3; i++) {
			try {
				HttpRESTful restful = HttpRESTful.open("POST", apiUrl);
				restful.setReadTimeout(60000);
				restful.setHeader("Content-Type", "application/json; charset=UTF-8");

				JSONObject json = new JSONObject();
				json.put("cid", cid);				
				json.put("sid", String.valueOf(encoder.id));
				json.put("enc_type", "elemental");				
				json.put("port_api", encoder.strPortApi );
				json.put("encIp", strReqIP );
				json.put("inputPath", "/mnt" + input.toString());
				
				EncodingItem encItem = outputs.get(0);
				

				JSONArray jsonArray = new JSONArray();
				
				//Dummy Preset 작업이 존재하면 Dummy Preset 명을 셋팅한다.
				if( strDummyPreset != null && !strDummyPreset.isEmpty() ) {
					json.put("dummy_preset", strDummyPreset );
					
					// Set Cid Ori 
					JSONArray jsonArrDummyCidOri = new JSONArray();
					for( String strCid : listDummyCidOri ) {
						jsonArrDummyCidOri.add( strCid );
					}
					json.put("dummy_cid_ori", jsonArrDummyCidOri );
					
					// Set Cid Type 0
					JSONArray jsonArrDummyCidType0 = new JSONArray();
					for( String strCid : listDummyCidType0 ) {
						jsonArrDummyCidType0.add( strCid );
					}
					json.put("dummy_cid_type_0", jsonArrDummyCidType0 );
					
					// Set Cid Type 1
					JSONArray jsonArrDummyCidType1 = new JSONArray();
					for( String strCid : listDummyCidType1 ) {
						jsonArrDummyCidType1.add( strCid );
					}
					json.put("dummy_cid_type_1", jsonArrDummyCidType1 );					
				}

				for (EncodingItem output : outputs) {
					if( output.eIDType == Define.IDType.HLS && ( output.strHlsEqualRtspCid != null && !output.strHlsEqualRtspCid.isEmpty() ) ) {
						logger.info( String.format("HLS Schedule Enc Type is Copy To RTSP Enc Result File | [ MediaID : %s ] [ ID : %s ] [ Framesize : %s ] [ Equal RTSP CID : %s ]", 
								output.strHlsMediaId,
								output.cid,
								output.framesize,
								output.strHlsEqualRtspCid
								) );
						continue;
					}
					
					JSONArray jsArrOpts = new JSONArray();
					JSONObject jsonArrayItem = new JSONObject();					
					JSONObject jsOptAdo = null;
					JSONObject jsOptHdr = null;					
					
					if( output.listAudioInfo.size() > 0 ) {	
						logger.info( String.format("Manual Setting Audio Cnt : %d", output.listAudioInfo.size() ) );
						jsOptAdo = new JSONObject();
						JSONArray jsArrAdo = new JSONArray();
						
						for( Object[] objAdo : output.listAudioInfo ) {
							JSONObject jsAdoItem = new JSONObject();
							jsAdoItem.put( "order", String.valueOf( (Integer)objAdo[0] ) );
							jsArrAdo.add( jsAdoItem );
						}
						
						jsOptAdo.put( "multi_audio", jsArrAdo );
						logger.info(new GsonBuilder().setPrettyPrinting().create().toJson(jsOptAdo));
						jsArrOpts.add( jsOptAdo );						
					}
					
					if( output.lEncOptHdrId > -1 ) {
						logger.info( String.format("Manual Setting Enc Opt ID : %d", output.lEncOptHdrId ) );
						jsOptHdr = new JSONObject();
						jsOptHdr.put("hdr", GetJsonEncOpt( output.lEncOptHdrId ) );
						logger.info(new GsonBuilder().setPrettyPrinting().create().toJson(jsOptHdr));
						jsArrOpts.add( jsOptHdr );						
					}
					
					if( jsArrOpts.isEmpty() ) {
						jsonArrayItem.put("manual_setting", null );
					}else {						
						jsonArrayItem.put("manual_setting", jsArrOpts );						
					}
					
					jsonArrayItem.put("cid", output.cid);
					String profile = null;
					if( isContent ) {
						profile = getEncodingProfile( output.eIDType == Define.IDType.RTSP ? "content" : "content_hls_enc", output.cid );
					}else {
						profile = getClipEncodingProfile(output.cid);
					}					
					jsonArrayItem.put("preset", profile);
					
					if (output.logoPath != null && output.logoPath.isEmpty() == false) {
						jsonArrayItem.put("logoPath", "/mnt" + output.logoPath);
						jsonArrayItem.put("copyright", "1");						
//						if (output.cautionPath != null) {
//							jsonArrayItem.put("cautionPath", output.cautionPath);
//							jsonArrayItem.replace("copyright", "2");
//						}
					}					
					
					if (output.smiPath != null && output.smiPath.isEmpty() == false) {
						jsonArrayItem.put("smiPath", "/mnt" + output.smiPath);
					}
					
					String outputPath = output.outputPath.toString();
					if (outputPath.endsWith("/") == false) {
						outputPath += "/";
					}
					
					jsonArrayItem.put("outputPath", "/mnt" + outputPath);
					jsonArrayItem.put("outputFilename", output.outputFilename);
					
					//확장자 구분 TS or MP4
					jsonArrayItem.put("ext", output.strExt );
					
					jsonArray.add(jsonArrayItem);
				}								
				json.put("output_list", jsonArray);

				logger.info( json.toString() );
				restful.setJson(json);
				JSONObject response = restful.connect();
				if (response == null) {
					continue;
				}
				return response;
			} catch (Exception e) {
				logger.error("", e);
			}	
		}

		throw new EmsException(ErrorCode.HTTP_OPEN_ERROR, apiUrl + " API 호출실패");
	}
	
	@SuppressWarnings("unchecked")
	protected JSONObject requestEncodingBySuperNova(String cid, SystemItem encoder, Path input, ArrayList<EncodingItem> outputs, Boolean isContent ) throws Exception {
		String eamsIp = null;
		DbTransaction db = new DbTransaction();		
		Connection conn = db.startAutoCommit();
		String strEncIP = null;
		HashMap<String, String> mapOptSnQuality = new HashMap<String, String>();
		try {
			mapOptSnQuality = new DbCommonHelper().GetMapDbDefineCode(conn, Define.MappingDefineCodeGrpCd.OPT_SN_QUALITY.toString() );
			if( mapOptSnQuality == null || mapOptSnQuality.isEmpty() ) {
				throw new Exception( "맵핑정의코드에 슈퍼노바옵션 코드가 정의되어 있지 않습니다." );
			}
			
			//strEncIP = new TaskReqReplaceIPSupernova().ChkIP( conn, encoder.ip );
			strEncIP = new ConvIP().excute(conn, ConvIPFlag.supernova, ConvIPReplaceFlag.c_class, encoder.ip );
			if( strEncIP == null || strEncIP.isEmpty() ) {
				throw new Exception( "Request Supernova 10G Convert Value is Empty" );
			}
			
			try (PreparedStatement pstmt = conn.prepareStatement("SELECT value FROM configure WHERE configure.key='eams_host'")) {
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {
						eamsIp = rs.getString(1);
					}
				}
			}	
		}catch( Exception ex ) {
			logger.error("",ex);
			throw ex;
		}finally {
			db.close();
		}

		String apiUrl = EmsPropertiesReader.get("eams.api.encoding").replace("*", eamsIp);	
				
		for (int i = 0; i < 3; i++) {
			try {
				HttpRESTful restful = HttpRESTful.open("POST", apiUrl);
				restful.setReadTimeout(60000);
				restful.setHeader("Content-Type", "application/json; charset=UTF-8");

				JSONObject json = new JSONObject();
				json.put("cid", cid);
				json.put("sid", String.valueOf(encoder.id));
				json.put("authorize_id", String.valueOf(encoder.strAuthorizeID));
				json.put("authorize_pw", String.valueOf(encoder.strAuthorizePW));
				json.put("enc_type", "supernova");
				json.put("encIp", strEncIP );
				json.put("port_api", encoder.strPortApi );				
				json.put("inputPath", input.toString());
				
				EncodingItem encItem = outputs.get(0);
				

				JSONArray jsonArray = new JSONArray();				

				for (EncodingItem output : outputs) {
					if( output.eIDType == Define.IDType.HLS && ( output.strHlsEqualRtspCid != null && !output.strHlsEqualRtspCid.isEmpty() ) ) {
						logger.info( String.format("HLS Schedule Enc Type is Copy To RTSP Enc Result File | [ MediaID : %s ] [ ID : %s ] [ Framesize : %s ] [ Equal RTSP CID : %s ]", 
								output.strHlsMediaId,
								output.cid,
								output.framesize,
								output.strHlsEqualRtspCid
								) );
						continue;
					}
					
					
					ProfileItem stItemProfile = getEncodingProfileItemBySuperNova( output.eIDType, output.cid );
					if( stItemProfile == null ) {
						throw new Exception( String.format("프로파일 정보를 조회하는데 실패하였습니다. | [ %s ]",output.cid ) );
					}
					
					JSONObject jsonArrayItem = new JSONObject();																				
					jsonArrayItem.put("cid", output.cid);
					jsonArrayItem.put("preset", stItemProfile.strName );
					jsonArrayItem.put("type", stItemProfile.strType );
					jsonArrayItem.put("resolution", stItemProfile.strResolution );
					
					// Opt Multi Audio
					if( output.listAudioInfo.size() > 0 ) {	
						logger.info( String.format("Manual Setting Audio Cnt : %d", output.listAudioInfo.size() ) );						
						JSONArray jsArrAdo = new JSONArray();
						
						for( Object[] objAdo : output.listAudioInfo ) {
							JSONObject jsAdoItem = new JSONObject();
							jsAdoItem.put( "order", String.valueOf( (Integer)objAdo[0] ) );
							jsArrAdo.add( jsAdoItem );
						}
																		
						jsonArrayItem.put( "opt_multi_audio", jsArrAdo );						
					}
					
					// Opt Logo
					if (output.logoPath != null && output.logoPath.isEmpty() == false) {
						jsonArrayItem.put("logoPath", output.logoPath);						
						JSONObject jsOptImage = new JSONObject();
						jsOptImage.put("location_x", stItemProfile.nOptSnImgLocationX );
						jsOptImage.put("location_y", stItemProfile.nOptSnImgLocationY );
						jsOptImage.put("width", stItemProfile.nOptSnImgWidth );
						jsOptImage.put("height", stItemProfile.nOptSnImgHeight );
						jsOptImage.put("opacity", stItemProfile.nOptSnImgOpacity );
						jsonArrayItem.put("opt_image", jsOptImage );
					}					
					
					// Opt Capt
					if (output.smiPath != null && output.smiPath.isEmpty() == false) {
						jsonArrayItem.put("smiPath", output.smiPath);
						JSONObject jsOptImage = new JSONObject();
						jsOptImage.put("font_size", stItemProfile.nOptSnCaptFontSize );
						jsOptImage.put("font_path", stItemProfile.strOptSnCaptFontPath );
						jsOptImage.put("font_location", stItemProfile.strOptSnCaptFontLocation );								
						jsOptImage.put("font_foreground_opacity", stItemProfile.nOptSnCaptFontForegroundOpacity );
						jsOptImage.put("font_foreground_color", stItemProfile.strOptSnCaptFontForegroundColor );
						jsOptImage.put("font_background_opacity", stItemProfile.nOptSnCaptFontBackgroundOpacity );
						jsOptImage.put("font_background_color", stItemProfile.strOptSnCaptFontBackgroundColor );
						jsOptImage.put("font_outline_size", stItemProfile.nOptSnCaptFontOutlineSize );
						jsOptImage.put("font_outline_color", stItemProfile.strOptSnCaptFontOutlineColor );						
						jsonArrayItem.put("opt_caption", jsOptImage );
					}
					
					String outputPath = output.outputPath.toString();
					if (outputPath.endsWith("/") == false) {
						outputPath += "/";
					}
					
					jsonArrayItem.put("outputPath", outputPath );
					jsonArrayItem.put("outputFilename", output.outputFilename);
					
					String strOptSnQuality = mapOptSnQuality.get( String.valueOf( output.nOptSnQuality ) );
					if( strOptSnQuality == null || strOptSnQuality.isEmpty() ) {
						throw new Exception( String.format("슈퍼노바코드에 정의되어 있지 않은 인코딩품질코드 입니다 | [ %d ]", output.nOptSnQuality ) );
					}
					
					jsonArrayItem.put("opt_sn_quality", strOptSnQuality );					
					
					//확장자 구분 TS or MP4
					jsonArrayItem.put("ext", output.strExt );
										
					jsonArray.add(jsonArrayItem);
				}								
				json.put("output_list", jsonArray);

				logger.info( json.toString() );
				restful.setJson(json);
				JSONObject response = restful.connect();
				if (response == null) {
					continue;
				}
				return response;
			} catch (Exception e) {
				logger.error("", e);
			}	
		}

		throw new EmsException(ErrorCode.HTTP_OPEN_ERROR, apiUrl + " API 호출실패");
	}

	protected String getEncodingProfile(String cid) {
		DbTransaction db = new DbTransaction();
		Connection conn = null;

		try {
			conn = db.startAutoCommit();			
			try (PreparedStatement pstmt = conn.prepareStatement("SELECT profile.name FROM content INNER JOIN profile on profile.id = content.profile_id WHERE cid=?")) {
				pstmt.setString(1, cid);
				logger.debug(pstmt.toString());
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {
						return rs.getString(1);
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
	
	public class ProfileItem
	{					
		public String strName;
		public String strType;
		public String strResolution;		
		
		//자막
		public int nOptSnCaptFontSize;
		public int nOptSnCaptFontForegroundOpacity;		
		public int nOptSnCaptFontBackgroundOpacity;		
		public int nOptSnCaptFontOutlineSize;
		public String strOptSnCaptFontPath;
		public String strOptSnCaptFontLocation;
		public String strOptSnCaptFontForegroundColor;
		public String strOptSnCaptFontBackgroundColor;
		public String strOptSnCaptFontOutlineColor;
		
		//로고
		public int nOptSnImgLocationX;
		public int nOptSnImgLocationY;
		public int nOptSnImgWidth;
		public int nOptSnImgHeight;
		public int nOptSnImgOpacity;
	}
	
	protected ProfileItem getEncodingProfileItemBySuperNova( Define.IDType eIDType, String cid ) {
		ProfileItem stRet = null;
		
		DbTransaction db = null;
		Connection conn = null;
		String sql = null;

		try {
			db = new DbTransaction();
			conn = db.startAutoCommit();				
						
			int nOriWidth = 0;
			int nOriHeight = 0;			
			
			// 원본파일 해상도 정보 얻기
			if( eIDType == IDType.RTSP ) {
				sql = "SELECT b.* FROM cems.content a INNER JOIN mediainfo b on b.id = a.mid WHERE a.cid=?";
			}else if( eIDType == IDType.HLS ) {
				sql = "SELECT b.* FROM cems.content_hls_enc a INNER JOIN mediainfo b on b.id = a.mid WHERE a.cid=?";
			}
			
			try (PreparedStatement pstmt = conn.prepareStatement( sql ) ) {
				pstmt.setString(1, cid);
				logger.debug(pstmt.toString());
				try( ResultSet rs = pstmt.executeQuery() ){
					if( rs.next() ) {
						String strInputRslu = rs.getString("input_resolution");
						if( strInputRslu == null || strInputRslu.isEmpty() ) {
							throw new Exception( String.format("원본 해상도 정보가 존재하지 않습니다. | [ %s ]", cid ) );
						}
						nOriWidth = Integer.valueOf( strInputRslu.split("x")[0] );
						nOriHeight = Integer.valueOf( strInputRslu.split("x")[1] );	
					}else {
						throw new Exception( String.format( "미디어정보 조회 실패 | [ %s ]", cid ) );
					}
				}
			}
			
			if( eIDType == IDType.RTSP ) {
				sql = "SELECT b.* FROM cems.content a INNER JOIN profile b on b.id = a.profile_id WHERE a.cid=?";
			}else if( eIDType == IDType.HLS ) {
				sql = "SELECT b.* FROM cems.content_hls_enc a INNER JOIN profile b on b.id = a.profile_id WHERE a.cid=?";
			}
			
			try (PreparedStatement pstmt = conn.prepareStatement( sql ) ) {
				pstmt.setString(1, cid);
				logger.debug(pstmt.toString());
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {						
						stRet = new ProfileItem();
						stRet.strName = rs.getString("b.name");;						
						stRet.strType = rs.getString("b.type");
						stRet.strResolution = rs.getString("b.resolution");
						
						// 슈파노바 옵션
						// 자막
						stRet.nOptSnCaptFontSize = rs.getInt("b.opt_sn_capt_font_size"); // 0일 경우 슈퍼노바는 자동
						stRet.strOptSnCaptFontPath = rs.getString("b.opt_sn_capt_font_path");
						stRet.strOptSnCaptFontLocation = rs.getString("b.opt_sn_capt_font_location");						
						stRet.nOptSnCaptFontForegroundOpacity = rs.getInt("b.opt_sn_capt_font_fore_opacity");
						stRet.strOptSnCaptFontForegroundColor = Util.ConvNullIsEmptyStr( rs.getString("b.opt_sn_capt_font_fore_color") );						
						stRet.nOptSnCaptFontBackgroundOpacity = rs.getInt("b.opt_sn_capt_font_back_opacity");
						stRet.strOptSnCaptFontBackgroundColor = Util.ConvNullIsEmptyStr( rs.getString("b.opt_sn_capt_font_back_color") );						
						stRet.nOptSnCaptFontOutlineSize = rs.getInt("b.opt_sn_capt_font_outline_size");
						stRet.strOptSnCaptFontOutlineColor = Util.ConvNullIsEmptyStr( rs.getString("b.opt_sn_capt_font_outline_color") );
						
						// 로고
						stRet.nOptSnImgLocationX = 0;
						stRet.nOptSnImgLocationY = 0;
						stRet.nOptSnImgWidth = nOriWidth; //원본파일 해상도 Width
						stRet.nOptSnImgHeight = nOriHeight; // 원본파일 해상도 Height
						stRet.nOptSnImgOpacity = rs.getInt("opt_sn_img_opacity");
					}else {
						throw new Exception( String.format( "프로파일 정보 조회 실패 | [ %s ]", cid ) );
					}
				}
			}
		} catch (Exception e) {
			logger.error("", e);			
		} finally {
			db.close();
		}
		
		return stRet;
	}
	
	protected String getEncodingProfile( String strTbl, String cid) {
		DbTransaction db = new DbTransaction();
		Connection conn = null;

		try {
			conn = db.startAutoCommit();			
			try (PreparedStatement pstmt = conn.prepareStatement( String.format( "SELECT b.name FROM %s a INNER JOIN profile b on b.id = a.profile_id WHERE a.cid=?", strTbl ) ) ) {
				pstmt.setString(1, cid);
				logger.debug(pstmt.toString());
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {
						return rs.getString(1);
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
	
	protected String getClipEncodingProfile(String cid) {
		DbTransaction db = new DbTransaction();
		Connection conn = null;

		try {
			conn = db.startAutoCommit();			
			try (PreparedStatement pstmt = conn.prepareStatement("SELECT profile.name FROM clip INNER JOIN profile on profile.id = clip.profile_id WHERE cid=?")) {
				pstmt.setString(1, cid);
				logger.debug(pstmt.toString());
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {
						return rs.getString(1);
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
	
	protected String getEncodingRatio(String cid, Define.IDType eIDType ) {
		String strRet = null;
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		String sql = null;

		try {
			conn = db.startAutoCommit();
			if( eIDType == Define.IDType.RTSP ) {
				sql = "SELECT profile.ratio FROM content INNER JOIN profile on profile.id = content.profile_id WHERE cid=?";
			}else if( eIDType == Define.IDType.HLS ) {
				sql = "SELECT profile.ratio FROM content_hls_enc INNER JOIN profile on profile.id = content_hls_enc.profile_id WHERE cid=?";
			}
			
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setString(1, cid);
				logger.debug(pstmt.toString());
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {
						strRet = rs.getString(1);
					}		
				}
			}
			
			if( strRet != null && !strRet.isEmpty() ) {
				return strRet;
			}
			
			if( eIDType == Define.IDType.RTSP ) {
				sql = "SELECT profile.opt_ratio_width, profile.opt_ratio_height FROM content INNER JOIN profile on profile.id = content.profile_id WHERE cid=?";
			}else if( eIDType == Define.IDType.HLS ) {
				sql = "SELECT profile.opt_ratio_width, profile.opt_ratio_height FROM content_hls_enc INNER JOIN profile on profile.id = content_hls_enc.profile_id WHERE cid=?";
			}
			
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setString(1, cid);
				logger.debug(pstmt.toString());
				try( ResultSet rs = pstmt.executeQuery() ){
					if (rs.next()) {
						int nRatioWidth =  rs.getInt(1);
						int nRatioHeight =  rs.getInt(2);
						if( nRatioWidth > 0 && nRatioHeight > 0 ) {
							strRet = String.format("%d:%d", nRatioWidth, nRatioHeight );
						}						
					}		
				}
			}
			
		} catch (Exception e) {
			logger.error("", e);			
		} finally {
			db.close();
		}
		return strRet;
	}
	
	
	protected String getEncodingParameterCidJsonTyeList( ArrayList<EncodingItem> outputs ) 
	{
		String result = null;
		JSONArray ja = new JSONArray();
		
		do{
			if( outputs.size() <= 0 ){
				break;
			}
			
			for( EncodingItem ei : outputs ){
				JSONObject js = new JSONObject();
				String temp = ei.cid.toString().replace("{", "\\{");
				temp = temp.replace("}", "\\}");
				temp = temp.replace("[", "\\[");
				temp = temp.replace("]", "\\]");
				js.put("cid", temp  );
				ja.add(js);
			}
			
			JSONObject rapperJson = new JSONObject();
			rapperJson.put("cids", ja);
					
			result = rapperJson.toJSONString().replace("\"", "\\\"");
			
		}while(false);
		
		return result;
	}
	
	private boolean MarkedEncodingCompletePath( ArrayList<EncodingItem> outputs, String strFramesize ) throws Exception
	{
		boolean bRet = false;
		
		ArrayList< String > listOriCid = new ArrayList<>();
		
		try {
			do {
				if( outputs == null || outputs.size() <= 0 ){
					logger.error("EncodingItem list is empty");
					break;
				}
				
				for( EncodingItem tempEncItem : outputs ){
					String strFramesizeChk = tempEncItem.framesize;
					if( strFramesizeChk == null || !strFramesizeChk.toUpperCase().equals(strFramesize) ){
						continue;
					}

					if( ( tempEncItem.extra_status & ExtraStatusManipulation.Flag.EF_REPLACE_JOB.get() ) != 0 && Util.IsLegacyReplaceJob( tempEncItem.cid ) ){
						//패치 이전의 교체작업 CID일 경우 DRM 작업용 CID가 없을 것이므로 바로 할당하자						
						listOriCid.add( tempEncItem.cid );
					}else{
						String strPureMoCid = Util.PureCidEx(tempEncItem.cid);
						if( tempEncItem.cid.equals( strPureMoCid ) ){
							listOriCid.add( tempEncItem.cid );							
						}
					}								
				}
				
				if( listOriCid.size() > 0 ){
					for( String strOriCid : listOriCid ){
						String strMark = "";
						
						for( EncodingItem tempEncItem : outputs ){
							String strFramesizeChk = tempEncItem.framesize;
							if( strFramesizeChk == null || !strFramesizeChk.toUpperCase().equals(strFramesize) ){
								continue;
							}
							
							
							if( tempEncItem.cid.contains( strOriCid ) ){
								if( !strMark.isEmpty() ){
									strMark += "|";
								}
								
								String profile = getEncodingProfile(tempEncItem.cid);
								strMark += String.format( "%s_%s.%s" , tempEncItem.outputFilename, profile, tempEncItem.strExt );
							}					
						}
						
						DbTransaction.updateDbSql( String.format("update cems.content set mark_enc_complete='%s' where cid='%s'", strMark, strOriCid ) );						
					}
				}	
				
				bRet = true;
				
			}while( false );			
		}catch( Exception ex ) {
			logger.error("",ex);
		}
		
		return bRet;
	}
	
	
	
	protected boolean MarkedMobileEncodingComplete( ArrayList<EncodingItem> outputs ) throws Exception
	{
		boolean bRet = false;
						
		try{
			
			do{
				if( outputs == null || outputs.size() <= 0 ){
					logger.error("EncodingItem list is empty");
					break;
				}
				
				HashMap<String, String> mapRslu = new DbCommonHelper().GetMapDbDefineCode(null, Define.MappingDefineCodeGrpCd.RSLU_CODE_MO.toString());
				if( mapRslu != null && !mapRslu.isEmpty() ) {
					
 					Collection<String> listRslu = mapRslu.values();
 					
 					for( String strRslu : listRslu ) {
 						MarkedEncodingCompletePath( outputs, strRslu.toUpperCase() ); 						
 					}
					
				}

				bRet = true;
				
			}while(false);			
			
		}catch( Exception ex ){
			logger.error("",ex);
			throw ex;
		}
		
		return bRet;
	}
		
	protected boolean MarkedTVEncodingComplete( ArrayList<EncodingItem> outputs ) throws Exception
	{
		boolean bRet = false;
						
		try{
			
			do{
				if( outputs == null || outputs.size() <= 0 ){
					logger.error("EncodingItem list is empty");
					break;
				}
				
				HashMap<String, String> mapRslu = new DbCommonHelper().GetMapDbDefineCode(null, Define.MappingDefineCodeGrpCd.RSLU_CODE_TV.toString());
				if( mapRslu != null && !mapRslu.isEmpty() ) {
					
 					Collection<String> listRslu = mapRslu.values();
 					
 					for( String strRslu : listRslu ) {
 						MarkedEncodingCompletePath( outputs, strRslu.toUpperCase() ); 						
 					}
					
				}				
								
				bRet = true;
				
			}while(false);			
			
		}catch( Exception ex ){
			logger.error("",ex);
			throw ex;
		}
		
		return bRet;
	}	
	
//	protected synchronized static boolean RemarkedEncComplete( String strCid, String strEncOutputFilePath, String strEncRealOutputFilePath ) throws Exception
//	{
//		boolean bRetAllComplete = false;
//		
//		String strMarkEncComplete = null;
//		String strEncOutputFileName = null;
//		
//		try{
//			if( strCid == null || strCid.isEmpty() ){
//				throw new Exception( "Cid is Empty" );
//			}
//			
//			if( strEncOutputFilePath == null || strEncOutputFilePath.isEmpty() ){
//				throw new Exception( "strEncOutputFilePath is Empty" );
//			}
//			
//			if( strEncRealOutputFilePath == null || strEncRealOutputFilePath.isEmpty() ){
//				throw new Exception( "strEncRealOutputFilePath is Empty" );
//			}
//			
//			strEncOutputFileName = Paths.get(strEncOutputFilePath).getFileName().toString();
//			
//			String strTaskCid = null;
//			if( Util.IsLegacyReplaceJob( strCid ) ){
//				strTaskCid = strCid;
//			}else{
//				strTaskCid = Util.PureCidEx(strCid);				
//			}
//			
//			if( strTaskCid == null || strTaskCid.isEmpty() ){
//				throw new Exception( String.format("Get Failure Task Remark Cid | [ Cid : %s ]", strCid) );
//			}			
//			
//			DbTransaction db = new DbTransaction();
//			Connection conn = null;
//
//			try {
//				conn = db.startAutoCommit();			
//				try (PreparedStatement pstmt = conn.prepareStatement("SELECT mark_enc_complete FROM cems.content WHERE cid=?")) {
//					pstmt.setString(1, strTaskCid);					
//					try( ResultSet rs = pstmt.executeQuery() ){
//						if (rs.next()) {
//							strMarkEncComplete = rs.getString(1);
//						}		
//					}
//				}
//			} catch (Exception e) {
//				logger.error("", e);
//				throw e;
//			} finally {
//				db.close();
//			}
//			
//			if( strMarkEncComplete == null || strMarkEncComplete.isEmpty() ){
//				throw new Exception( String.format("MarkEncComplete is Empty | [ Cid : %s ] [ Task CID : %s ]", strCid, strTaskCid) );
//			}
//			
//			strMarkEncComplete = strMarkEncComplete.replace( strEncOutputFileName.toString(), "");
//			String[] strSplitEncCopyChk = strMarkEncComplete.split("[|]");
//			if( strMarkEncComplete.isEmpty() || strSplitEncCopyChk.length <= 0 ){
//				// Enc All Complete				
//				DbTransaction.updateDbSql( String.format("update cems.content set mark_enc_complete=null where cid='%s'", strTaskCid ) );				
//				bRetAllComplete = true;
//				logger.info(String.format("Enc Task All Complete | [ cid : %s ]", strCid));				
//			}else{
//				// Remaind Enc Task
//				DbTransaction.updateDbSql( String.format("update cems.content set mark_enc_complete='%s' where cid='%s'", strMarkEncComplete, strTaskCid ) );
//				logger.info(String.format("Remaind Enc Task... | [ cid : %s ]", strCid));
//			}				
//			
//		}catch( Exception ex ){
//			logger.error("",ex);
//			throw ex;
//		}
//		
//		return bRetAllComplete;
//	}
	
	
	protected synchronized static boolean RemarkedEncCompleteEx( String strCid, String strEncOutputFilePath, String strEncRealOutputFilePath ) throws Exception
	{
		boolean bRetAllComplete = false;
		
		String strMarkEncComplete = null;
		String strEncOutputFileName = null;
		
		try{
			if( strCid == null || strCid.isEmpty() ){
				throw new Exception( "Cid is Empty" );
			}
			
			if( strEncOutputFilePath == null || strEncOutputFilePath.isEmpty() ){
				throw new Exception( "strEncOutputFilePath is Empty" );
			}
			
			if( strEncRealOutputFilePath == null || strEncRealOutputFilePath.isEmpty() ){
				throw new Exception( "strEncRealOutputFilePath is Empty" );
			}
			
			strEncOutputFileName = Paths.get(strEncOutputFilePath).getFileName().toString();
			
			Define.IDType idType = null;
			String strTaskCid = null;
			if( Util.IdTypeIsRTSP( strCid ) ) {
				if( Util.IsLegacyReplaceJob( strCid ) ){
					strTaskCid = strCid;
				}else{
					strTaskCid = Util.PureCidEx(strCid);				
				}	
				
				idType = Define.IDType.RTSP;
			}else {
				strTaskCid = strCid;
				idType = Define.IDType.HLS;
			}
			
			if( strTaskCid == null || strTaskCid.isEmpty() ){
				throw new Exception( String.format("Get Failure Task Remark Cid | [ Cid : %s ]", strCid) );
			}			
			
			DbTransaction db = new DbTransaction();
			Connection conn = null;

			try {
				conn = db.startAutoCommit();			
				try (PreparedStatement pstmt = conn.prepareStatement("SELECT mark_enc_complete FROM cems.content WHERE cid=?")) {
					pstmt.setString(1, strTaskCid);					
					try( ResultSet rs = pstmt.executeQuery() ){
						if (rs.next()) {
							strMarkEncComplete = rs.getString(1);
						}		
					}
				}
			} catch (Exception e) {
				logger.error("", e);
				throw e;
			} finally {
				db.close();
			}
			
			do {
				if( strMarkEncComplete == null || strMarkEncComplete.isEmpty() ){
					logger.info( String.format("MarkEncComplete is Empty | [ Cid : %s ] [ Task CID : %s ]", strCid, strTaskCid) );
					bRetAllComplete = true;
					break;
				}
				
				strMarkEncComplete = strMarkEncComplete.replace( strEncOutputFileName.toString(), "");
				String[] strSplitEncCopyChk = strMarkEncComplete.split("[|]");
				if( strMarkEncComplete.isEmpty() || strSplitEncCopyChk.length <= 0 ){
					// Enc All Complete				
					DbTransaction.updateDbSql( String.format("update cems.content set mark_enc_complete=null where cid='%s'", strTaskCid ) );				
					bRetAllComplete = true;
					logger.info(String.format("Enc Task All Complete | [ cid : %s ]", strCid));				
				}else{
					// Remaind Enc Task
					DbTransaction.updateDbSql( String.format("update cems.content set mark_enc_complete='%s' where cid='%s'", strMarkEncComplete, strTaskCid ) );
					logger.info(String.format("Remaind Enc Task... | [ cid : %s ]", strCid));
				}	
			}while( false );						
			
		}catch( Exception ex ){
			logger.error("",ex);
			throw ex;
		}
		
		return bRetAllComplete;
	}
	
	protected class EncCompleteItem
	{
		public String strHlsMediaId;
		public String strCid;		
		public String strEncOutputFilePath;
		public String strEncRealOutputFilePath;
		public String strType;
		public String strFramesize;
		public String str1stFilePath;
		public String rcmd_brt_typ_cd;
		public String strEpsdID;
		public String strPossnYN;
		public long lExtraStatus;
		public int encd_piqu_typ_cd;
		public int encd_qlty_res_cd = 0;
		public int nSubUsage = 0;
		public ArrayList<String> listMdaRsluId = new ArrayList<String>();
		Define.IDType eIdType;		
		
		public EncCompleteItem(){}
		public EncCompleteItem( Define.IDType eIdType, 
								String strCid, 
								String strEncOutputFilePath, 
								String strEncRealOutputFilePath, 
								long lExtraStatus, 
								String strType, 
								String strHlsMediaId, 
								int encd_piqu_typ_cd, 
								String strFramesize, 
								String str1stFilePath 
								)
		{
			this.eIdType = eIdType;
			this.strCid = strCid;
			this.strEncOutputFilePath = strEncOutputFilePath;
			this.strEncRealOutputFilePath = strEncRealOutputFilePath;
			this.lExtraStatus = lExtraStatus;
			this.strType = strType;
			this.strHlsMediaId = strHlsMediaId;
			this.encd_piqu_typ_cd = encd_piqu_typ_cd;
			this.strFramesize = strFramesize;
			this.str1stFilePath = str1stFilePath;			
		}
	}
		
	protected ArrayList<EncCompleteItem> GetRelationEncOutputInfoEx( String strCid ) throws Exception
	{		
		ArrayList<EncCompleteItem> listRet = new ArrayList<>();
		
		try{
			if( strCid == null || strCid.isEmpty() ){
				throw new Exception( String.format("Cid is Empty") );
			}
			
			Define.IDType idType = null;
			String strTaskCid = null;			
			if( Util.IdTypeIsRTSP( strCid ) ) {				
				strTaskCid = strCid;								
				idType = Define.IDType.RTSP;
			}else {
				strTaskCid = strCid; 
				idType = Define.IDType.HLS;
			}			
						
			if( strTaskCid == null || strTaskCid.isEmpty() ){
				throw new Exception( String.format("Get Failure Task Pure Cid | [ ID Type : %s ] [ Cid : %s ]", idType.toString(), strCid) );
			}
			
			DbTransaction db = new DbTransaction();
			Connection conn = null;

			try {
				conn = db.startAutoCommit();	
				
				
				if( idType == Define.IDType.RTSP ) {
					
					boolean bIsTV = false;
					String strEqualHlsCid = null;
					
					try (PreparedStatement pstmt = conn.prepareStatement("SELECT equal_hls_cid, type FROM cems.content WHERE cid = ?")) {						
						pstmt.setString(1, strTaskCid );
						logger.info(pstmt.toString());
						try( ResultSet rs = pstmt.executeQuery() ){
							if(rs.next()) {								
								String strType = rs.getString("type");
								if( strType != null && strType.toUpperCase().equals("TV") ) {
									bIsTV = true;
									strEqualHlsCid = rs.getString("equal_hls_cid");
								}								
							}		
						}
					}					
				
					if( bIsTV ) {
						
						if( strEqualHlsCid != null && !strEqualHlsCid.isEmpty() ) {
							try (PreparedStatement pstmt = conn.prepareStatement("SELECT encd_qlty_res_cd, mda_id, rcmd_brt_typ_cd, 1st_filepath, framesize, encd_piqu_typ_cd, type, extra_status, cid, enc_output_path, enc_real_output_path FROM cems.content_hls_enc WHERE cid=?")) {								
								pstmt.setString(1, strEqualHlsCid );								
								logger.info(pstmt.toString());
								try( ResultSet rs = pstmt.executeQuery() ){
									while(rs.next()) {
										EncCompleteItem encCmpItem = new EncCompleteItem();										
										encCmpItem.eIdType = Define.IDType.HLS;
										encCmpItem.strCid = rs.getString( "cid" );
										encCmpItem.strEncOutputFilePath = rs.getString( "enc_output_path" );
										encCmpItem.strEncRealOutputFilePath = rs.getString( "enc_real_output_path" );
										encCmpItem.lExtraStatus = rs.getLong("extra_status");
										encCmpItem.strType = rs.getString("type");
										encCmpItem.strHlsMediaId = rs.getString("mda_id");
										encCmpItem.encd_piqu_typ_cd = rs.getInt("encd_piqu_typ_cd");
										encCmpItem.strFramesize = rs.getString("framesize");
										encCmpItem.str1stFilePath = rs.getString("1st_filepath");
										encCmpItem.rcmd_brt_typ_cd = rs.getString("rcmd_brt_typ_cd");
										encCmpItem.encd_qlty_res_cd = rs.getInt("encd_qlty_res_cd");
										listRet.add( encCmpItem );
									}		
								}
							}	
						}
						
					}else {
						// 가상 CID 내역 먼저 리스트 업
						try (PreparedStatement pstmt = conn.prepareStatement("SELECT possn_yn, encd_qlty_res_cd, mda_rslu_id, epsd_id, sub_usage, rcmd_brt_typ_cd, 1st_filepath, framesize, encd_piqu_typ_cd, type, extra_status, cid, enc_output_path, enc_real_output_path FROM cems.content WHERE ( extra_status & ? ) != 0 and cid like ? and cid != ?")) {
							pstmt.setLong(1, ExtraStatusManipulation.Flag.EF_NEW_DRM.get() | ExtraStatusManipulation.Flag.EF_WATERMARK.get() | ExtraStatusManipulation.Flag.EF_ADD_ECDN_TASK.get() );
							pstmt.setString(2, Util.pureCidEx( strTaskCid ) + '%' );
							pstmt.setString(3, Util.pureCidEx( strTaskCid ) );
							logger.info(pstmt.toString());
							try( ResultSet rs = pstmt.executeQuery() ){
								while (rs.next()) {
									EncCompleteItem encCmpItem = new EncCompleteItem();
									encCmpItem.eIdType = Define.IDType.RTSP;
									encCmpItem.strCid = rs.getString( "cid" );
									encCmpItem.strEncOutputFilePath = rs.getString( "enc_output_path" );
									encCmpItem.strEncRealOutputFilePath = rs.getString( "enc_real_output_path" );
									encCmpItem.lExtraStatus = rs.getLong("extra_status");
									encCmpItem.strType = rs.getString("type");
									encCmpItem.strHlsMediaId = "";
									encCmpItem.encd_piqu_typ_cd = rs.getInt("encd_piqu_typ_cd");
									encCmpItem.strFramesize = rs.getString("framesize");
									encCmpItem.str1stFilePath = rs.getString("1st_filepath");
									encCmpItem.rcmd_brt_typ_cd = rs.getString("rcmd_brt_typ_cd");
									encCmpItem.nSubUsage = rs.getInt("sub_usage");
									encCmpItem.strEpsdID = rs.getString("epsd_id");
									encCmpItem.encd_qlty_res_cd = rs.getInt("encd_qlty_res_cd");
									encCmpItem.strPossnYN = rs.getString("possn_yn");
									encCmpItem.listMdaRsluId.addAll( Arrays.asList( Util.ConvNullIsEmptyStr( rs.getString("mda_rslu_id") ).split("[|]") ) );									
									listRet.add( encCmpItem );
								}		
							}
						}
						
					}
						
					
					// TS 내역 리스트업
					try (PreparedStatement pstmt = conn.prepareStatement("SELECT possn_yn, encd_qlty_res_cd, mda_rslu_id, epsd_id, sub_usage, rcmd_brt_typ_cd, 1st_filepath, framesize, encd_piqu_typ_cd, type, extra_status, cid, enc_output_path, enc_real_output_path FROM cems.content WHERE cid=?")) {
						pstmt.setString(1, Util.pureCidEx( strTaskCid ) );
						logger.info(pstmt.toString());
						try( ResultSet rs = pstmt.executeQuery() ){
							if (rs.next()) {
								EncCompleteItem encCmpItem = new EncCompleteItem();
								encCmpItem.eIdType = Define.IDType.RTSP;
								encCmpItem.strCid = rs.getString( "cid" );
								encCmpItem.strEncOutputFilePath = rs.getString( "enc_output_path" );
								encCmpItem.strEncRealOutputFilePath = rs.getString( "enc_real_output_path" );
								encCmpItem.lExtraStatus = rs.getLong("extra_status");
								encCmpItem.strType = rs.getString("type");
								encCmpItem.strHlsMediaId = "";
								encCmpItem.encd_piqu_typ_cd = rs.getInt("encd_piqu_typ_cd");
								encCmpItem.strFramesize = rs.getString("framesize");
								encCmpItem.str1stFilePath = rs.getString("1st_filepath");
								encCmpItem.rcmd_brt_typ_cd = rs.getString("rcmd_brt_typ_cd");
								encCmpItem.nSubUsage = rs.getInt("sub_usage");
								encCmpItem.strEpsdID = rs.getString("epsd_id");
								encCmpItem.encd_qlty_res_cd = rs.getInt("encd_qlty_res_cd");
								encCmpItem.strPossnYN = rs.getString("possn_yn");
								encCmpItem.listMdaRsluId.addAll( Arrays.asList( Util.ConvNullIsEmptyStr( rs.getString("mda_rslu_id") ).split("[|]") ) );
								listRet.add( encCmpItem );
							}		
						}
					}
					
				}else if( idType == Define.IDType.HLS ) {
					
					try (PreparedStatement pstmt = conn.prepareStatement("SELECT encd_qlty_res_cd, mda_id, rcmd_brt_typ_cd, 1st_filepath, framesize, encd_piqu_typ_cd, mda_id, type, extra_status, cid, enc_output_path, enc_real_output_path FROM cems.content_hls_enc WHERE cid=?")) {								
						pstmt.setString(1, strTaskCid );								
						logger.info(pstmt.toString());
						try( ResultSet rs = pstmt.executeQuery() ){
							while(rs.next()) {
								EncCompleteItem encCmpItem = new EncCompleteItem();
								encCmpItem.eIdType = Define.IDType.HLS;
								encCmpItem.strCid = rs.getString( "cid" );
								encCmpItem.strEncOutputFilePath = rs.getString( "enc_output_path" );
								encCmpItem.strEncRealOutputFilePath = rs.getString( "enc_real_output_path" );
								encCmpItem.lExtraStatus = rs.getLong("extra_status");
								encCmpItem.strType = rs.getString("type");
								encCmpItem.strHlsMediaId = rs.getString("mda_id");
								encCmpItem.encd_piqu_typ_cd = rs.getInt("encd_piqu_typ_cd");
								encCmpItem.strFramesize = rs.getString("framesize");
								encCmpItem.str1stFilePath = rs.getString("1st_filepath");
								encCmpItem.rcmd_brt_typ_cd = rs.getString("rcmd_brt_typ_cd");
								encCmpItem.encd_qlty_res_cd = rs.getInt("encd_qlty_res_cd");
								listRet.add( encCmpItem );
							}
						}
					}
					
				}				
				
			} catch (Exception e) {
				logger.error("", e);
				throw e;
			} finally {
				db.close();
			}
			
		}catch( Exception ex ){
			logger.error("",ex);
			throw ex;
		}
		
		return listRet;
	}
	
	protected boolean ClearEncOutPathInfo( ArrayList<EncCompleteItem> listEncItem ) 
	{
		boolean bRet = false;
		
		DbTransaction db = new DbTransaction();
		Connection conn = null;
		
		try{
			conn = db.start();
			
			for( EncCompleteItem tempEncItem : listEncItem ){
				DbTransaction.updateDbSql(conn, String.format("update cems.content set enc_output_path=null, enc_real_output_path=null where cid='%s'", tempEncItem.strCid) );
			}
			
			bRet = true;				
			db.commit();
		}catch( Exception ex ){
			db.rollback();				
			logger.error("",ex);
		}finally{
			db.close();
		}
		
		return bRet;
	}
	
	protected boolean ContinueEncTask( String cid ) throws Exception
	{
		boolean bRet = true;
		DbTransaction db = null;
		Connection conn = null;
		
		try{
			if( cid == null || cid.isEmpty() ){
				throw new Exception( "cid is empty" );
			}
						
			try {
				db = new DbTransaction();				
				conn = db.startAutoCommit();		
				
				try (PreparedStatement pstmt = conn.prepareStatement("SELECT content_status FROM cems.content WHERE cid like ?")) {
					pstmt.setString(1, Util.pureCidEx(cid) + '%' );					
					logger.info(pstmt.toString());
					try( ResultSet rs = pstmt.executeQuery() ){
						while (rs.next()) {
							String strContentStatus = rs.getString(1);
							if( strContentStatus == null || strContentStatus.isEmpty() || strContentStatus.equals("작업오류") ){
								bRet = false;
								break;
							}														
						}		
					}
				}
								
			} catch (Exception e) {
				logger.error("", e);
				throw e;
			} finally {
				db.close();
			}
			
		}catch( Exception ex ){
			logger.error("",ex);
			throw ex;
		}
		
		return bRet;
	}
	
	
	public boolean IsNeedCheckIsExistEncCaptionFile( String strCid, Define.IDType idType, boolean bNeedCaptionTask, boolean bVttExtraction )throws Exception
	{
		boolean bRet = false;
		
		try {
			do {
				if( strCid == null || strCid.isEmpty() ) {
					throw new Exception( "Param strCid is Empty" );
				}
				
				if( idType == Define.IDType.RTSP ) {
					
					if( !bNeedCaptionTask ) {
						logger.info(String.format("[ IsNeedCheckIsExistEncCaptionFile ] Not Need Caption!!! | [ ID Type : %s ] [ ID : %s ] [ bNeedCaptionTask : %s ]", 
								idType.toString(),
								strCid, 												 
								bNeedCaptionTask ? "True" : "False" 																			
								));
						break;
					}
					
				}else {
										
					if( !bNeedCaptionTask ) {
						logger.info(String.format("[ IsNeedCheckIsExistEncCaptionFile ] Not Need Caption!!! | [ ID Type : %s ] [ ID : %s ] [ bNeedCaptionTask : %s ]", 
								idType.toString(),
								strCid, 												 
								bNeedCaptionTask ? "True" : "False" 																			
								));
						break;
					}
					
					if( !bVttExtraction ) {
						logger.info(String.format("[ IsNeedCheckIsExistEncCaptionFile ] Not Need Caption, is Not Low Quality HLS CID!!! | [ ID Type : %s ] [ ID : %s ] [ bNeedCaptionTask : %s ] [ bVttExtraction : %s ]", 
								idType.toString(),
								strCid, 												 
								bNeedCaptionTask ? "True" : "False",
								bVttExtraction ? "True" : "False"
								));
						break;
					}
					
				}			
				
				logger.info(String.format("[ IsNeedCheckIsExistEncCaptionFile ] Need Caption OK !!! | [ ID Type : %s ] [ ID : %s ] [ bNeedCaptionTask : %s ] [ bVttExtraction : %s ]", 
											idType.toString(),
											strCid, 												 
											bNeedCaptionTask ? "True" : "False",
											bVttExtraction ? "True" : "False"		
											));
								
				bRet = true;
			}while( false );			
		}catch( Exception ex ) {
			logger.error("",ex);
			throw ex;
		}
		
		return bRet;
	}
	
	public static boolean MoveOutputVttFile( String strSrcPath, String strDestPath ) throws Exception
	{
		boolean bRet = false;
		try {
			do {
				logger.info( String.format("[ MoveOutputVttFile ] Param From : %s | Param To : %s ", strSrcPath, strDestPath ) );
				
				if( strSrcPath == null || strSrcPath.isEmpty() ) {
					logger.error("Param strSrcPath is Empty");
					break;
				}
				
				if( strDestPath == null || strDestPath.isEmpty() ) {
					logger.error("Param strDestPath is Empty");
					break;
				}
				
				if( !Util.isExistFile( strSrcPath ) ) {
					logger.error( String.format( "Src Path File is Not Exsit, Skip Output Vtt File Move Proc | [ Src Path : %s ]", strSrcPath) );
					break;
				}
				
				if( !new MoveFileIo(strSrcPath, strDestPath).start(null) ) {
					logger.error( String.format("[ MoveOutputVttFile ]  Output Vtt File Move Filaure | [ From : %s ] >>>> [ To : %s ]", strSrcPath, strDestPath ) );
					break;
				}
				
				logger.error( String.format("[ MoveOutputVttFile ]  Output Vtt File Move Success !! | [ From : %s ] >>>> [ To : %s ]", strSrcPath, strDestPath ) );
				
				bRet = true;
				
			}while( false );			
		}catch( Exception ex ) {
			logger.error("",ex);			
		}
		return bRet;
	}
	
	
	public static String GetEncodingCautionPath( String strCid, String strCopyright, boolean bAlreadyCopyToCautionFile, SystemItem system ) throws Exception
	{
		String strRet = "";
		
		try {
			do {
				if( strCid == null || strCid.isEmpty() ) {
					throw new Exception( "Param strCid is Empty" );
				}
				
				if( system == null ) {
					throw new Exception( "Param system is Empty" );
				}
				
				if( strCopyright == null || strCopyright.isEmpty() || !strCopyright.contains("경고문") ) {
					logger.info( String.format( "[ GetEncodingCautionPath ] Param strCopyright is Not Contain 경고문, Skip GetEncOutputCautionPath | [ CID : %s ] [ Copyright : %s ]", strCid, strCopyright ) );
					break;
				}
				
				//
				// /64/원본/[Material]/자막-경고문.smi => /64/원본/[원본]2-작업/[Subtitle]/ 로 복사
				//
				Path cautionPath = Paths.get(EmsPropertiesReader.get("material.caution.path"));
				String from = cautionPath.toString();
				
				Path pDirCaution = Paths.get(EmsPropertiesReader.get("[원본]작업.io.dest")).resolve("[Subtitle]").resolve( system.name );
				if( !Util.isExistFolder( pDirCaution.toString() ) ) {
					Util.createFolder( pDirCaution.toString() );
				}
				
				String to = pDirCaution.resolve(cautionPath.getFileName()).toString();
				
				// 경고문은 1개이므로 한번만 복사한다.
				if (!bAlreadyCopyToCautionFile) {					
					copyFile(from, to);
					logger.info( String.format("[ GetEncodingCautionPath ] Copy to CautionFile | [ CID : %s ] [ From : %s ] >>>> [ To : %s ]", strCid, from, to ) );
				}else {
					logger.info( String.format("[ GetEncodingCautionPath ] Already Copy to CautionFile, Skip Copy Proc | [ CID : %s ]", strCid ) );
				}
				
				strRet = to;
				
				logger.info( String.format("[ GetEncodingCautionPath ] Get CautionFile OK | [ CID : %s ] [ Path : %s ]", strCid, to ) );
				
			}while( false );
						
		}catch( Exception ex ) {
			logger.error("",ex);
			throw ex;
		}
		
		return strRet;
	}
	
	protected String GetEcdnTaskLowQualityCidHls( ArrayList<EncodingItem> outputs ) throws Exception
	{
		String strRet = "";
						
		ArrayList<Define.IDItem> listRtspItem = new ArrayList<>();
		ArrayList<Define.IDItem> listHlsItem = new ArrayList<>();
				
		try{
			HashMap<String, String> mapFramePrio = new DbCommonHelper().GetMapDbDefineCode(null, Define.MappingDefineCodeGrpCd.FRAME_PRIO_SCORE.toString() );
			if( mapFramePrio == null || mapFramePrio.isEmpty() ) {
				throw new Exception( String.format("Get Failure Db Mapping Code | [ %s ]", Define.MappingDefineCodeGrpCd.FRAME_PRIO_SCORE.toString() ) );
			}
			
			for( EncodingItem encItem : outputs ) {
				if( encItem.eIDType == Define.IDType.RTSP ) {
					listRtspItem.add( new Define.IDItem( encItem.cid, encItem.eIDType ) );
				}else if( encItem.eIDType == Define.IDType.HLS ) {
					listHlsItem.add( new Define.IDItem( encItem.cid, encItem.eIDType ) );
				}
			}
			
			if( listRtspItem.size() > 0 && listHlsItem.size() > 0 ) {
				// HLS 기준으로 진행
				strRet = new DbCommonHelper().GetFramePrio(null, listHlsItem, false).strID;
			}else if( listRtspItem.size() > 0 && listHlsItem.size() <= 0 ) {
				// RTSP 기준으로 진행
				strRet = "";
			}else if( listRtspItem.size() <= 0 && listHlsItem.size() > 0 ) {
				// HLS 기준으로 진행
				strRet = new DbCommonHelper().GetFramePrio(null, listHlsItem, false).strID;
			}
			
		}catch( Exception ex ){
			logger.error("",ex);			
		}
		
		return strRet;
	}
	
	
	protected ArrayList<String> GetSameTaskingEcdnCIDs( ArrayList<EncodingItem> outputs ) throws Exception
	{
		ArrayList< String > listRet = new ArrayList<>();
				
		DbTransaction db = null;
		Connection conn = null;
		
		try{			
			try {
				db = new DbTransaction();				
				conn = db.startAutoCommit();		
				
				try (PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM cems.content WHERE cid=?")) {
					for( EncodingItem encodingItem : outputs ){
						pstmt.setString(1, encodingItem.cid );					
						logger.info(pstmt.toString());
						try( ResultSet rs = pstmt.executeQuery() ){
							while (rs.next()) {
								
								String strSetNum = rs.getString("set_num");
								if( strSetNum != null && !strSetNum.isEmpty() ) {
									
									try (PreparedStatement pstmt2 = conn.prepareStatement("SELECT * FROM cems.content WHERE set_num=?")) {										
										pstmt2.setString(1, strSetNum );					
										logger.info(pstmt.toString());
										try( ResultSet rs2 = pstmt2.executeQuery() ){
											while (rs2.next()) {
												
												long lExtraStatus2 = rs2.getLong("extra_status");
												if( ( lExtraStatus2 & ExtraStatusManipulation.Flag.EF_ADD_ECDN_TASK.get() ) == 0 ) {
													continue;									
												}
																								
												if( !listRet.contains( rs2.getString("cid") ) ) {
													listRet.add( rs2.getString("cid") );
												}												
												
											}
										}
									}
									
								} else {
									
									long lExtraStatus = rs.getLong("extra_status");
									if( ( lExtraStatus & ExtraStatusManipulation.Flag.EF_ADD_ECDN_TASK.get() ) == 0 ) {
										continue;									
									}
									
									if( !listRet.contains( rs.getString("cid") ) ) {
										listRet.add( rs.getString("cid") );
									}
									
								}
								
							}		
						}	
					}					
				}				
							
			} catch (Exception e) {
				logger.error("", e);
				throw e;
			} finally {
				db.close();
			}
			
		}catch( Exception ex ){
			logger.error("",ex);			
		}
		
		return listRet;
	}
	
	
	protected boolean RemoveMobileQualityOutputInfo( ArrayList<EncodingItem> tempListOutput ) throws Exception
	{
		boolean bRet = false;
		
		try{
			Iterator<EncodingItem> iterator = tempListOutput.iterator();
			while (iterator.hasNext()) {
				EncodingItem encItem = iterator.next();
				if( encItem.strType != null && encItem.strType.toLowerCase().equals("mobile") ){	
					logger.info( String.format("Remove Mobile Quality | [ cid : %s ]", encItem.cid ) );
					iterator.remove();
					bRet = true;
					break;
				}
			}
			
		}catch( Exception ex ){
			logger.error("",ex);
			throw ex;
		}
		
		return bRet;
	}
	
	protected boolean RemoveTVEcdnOutputCopyFileOutputInfo( ArrayList<EncodingItem> tempListOutput ) throws Exception
	{
		boolean bRet = false;
		
		try{
			Iterator<EncodingItem> iterator = tempListOutput.iterator();
			while (iterator.hasNext()) {
				EncodingItem encItem = iterator.next();
				boolean bIsEcdnOutputCopyFile = ( encItem.extra_status & ExtraStatusManipulation.Flag.EF_ADD_ECDN_ENC_COPY_OUTPUT_FILE.get() ) != 0 ? true : false;				
				if( encItem.strType != null && encItem.strType.toLowerCase().equals("tv") && bIsEcdnOutputCopyFile ){	
					logger.info( String.format("Remove TV E-CDN Enc Output Copy File | [ cid : %s ]", encItem.cid ) );
					iterator.remove();
					bRet = true;
					break;
				}
			}
			
		}catch( Exception ex ){
			logger.error("",ex);
			throw ex;
		}
		
		return bRet;
	}
	
	
	protected boolean RemoveTVQualityOutputInfo( ArrayList<EncodingItem> tempListOutput ) throws Exception
	{
		boolean bRet = false;
		
		try{
			Iterator<EncodingItem> iterator = tempListOutput.iterator();
			while (iterator.hasNext()) {
				EncodingItem encItem = iterator.next();
				if( encItem.strType != null && encItem.strType.toUpperCase().equals("TV") ){	
					logger.info( String.format("TvMobileEncodingPipeline Remove TV Quality | [ cid : %s ]", encItem.cid ) );
					iterator.remove();
					bRet = true;
					break;
				}
			}
			
		}catch( Exception ex ){
			logger.error("",ex);
			throw ex;
		}
		
		return bRet;
	}
	
	protected boolean IsExistEcdnEncOutputCopyFileOptEx( String strCid ) throws Exception
	{
		boolean bRet = false;
		
		DbTransaction db = null;
		Connection conn = null;
		
		try {
			if( strCid == null || strCid.isEmpty() ) {
				throw new Exception( "Param CID Is Empty" );
			}
			
			do {
			
				if( !Util.IdTypeIsRTSP( strCid ) ) {
					logger.info( String.format("ID Type is HLS, EncOutputCopyFileOpt Proc Skip !! | [ %s ] ", strCid ) );				
					break;
				}
				
				String strPureCid = Util.pureCidEx(strCid);
				if( strPureCid == null || strPureCid.isEmpty() ) {
					throw new Exception( "Param Pure CID Is Empty" );
				}
				
				logger.info( String.format("[ IsExistEcdnEncOutputCopyFileOpt ] Check Pure CID : %s", strCid ) );
				
				db = new DbTransaction();
				conn = db.startAutoCommit();
				
				try (PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM content WHERE cid like ?")) {				
					pstmt.setString(1, strPureCid + '%');
					logger.info( pstmt.toString() );
					try( ResultSet rs = pstmt.executeQuery()){
						while( rs.next() ){
							String strType = rs.getString("type");
							if( strType == null || !strType.toUpperCase().equals("TV") ) {
								continue;
							}
							
							String strEqualHlsCid = rs.getString( "equal_hls_cid" );
							if( strEqualHlsCid != null && !strEqualHlsCid.isEmpty() ) {
								logger.info( String.format("[ IsExistEcdnEncOutputCopyFileOpt ] Is Exist RTSP Encoding Output Copy File Opt | [ RTSP CID : %s ] [ HLS ID : %s ]", 
															rs.getString("cid"),
															strEqualHlsCid
															) );
								bRet = true;
								break;
							}													
						}
					}				
				}catch( Exception ex ) {
					logger.error("",ex);
					throw ex;
				}finally {
					db.close();
				}
				
			}while( false );
			
		}catch( Exception ex ) {
			logger.error("",ex);
			throw ex;
		}
		
		return bRet;
	}
	
	protected boolean SetResultEcdnEncOutputCopyFileInfoEx( String strCid ) throws Exception
	{
		boolean bRet = false;
		
		DbTransaction db = null;
		Connection conn = null;
		String strEncOutputPath = null;
		String strEncOutputPathReal = null;
		String strFramesize = null;
		String strType = null;
		String strPossnYn = null;
		String strEqualHlsCid = null;
		
		try {
			if( strCid == null || strCid.isEmpty() ) {
				throw new Exception( "Param CID Is Empty" );
			}
			
			if( !Util.IdTypeIsRTSP( strCid ) ) {
				throw new Exception( String.format("ID Type is Not RTSP | [ %s ]", strCid ) );
			}
			
			String strPureCid = Util.pureCidEx(strCid);
			if( strPureCid == null || strPureCid.isEmpty() ) {
				throw new Exception( "Param Pure CID Is Empty" );
			}
			
			logger.info( String.format("[ IsExistEcdnEncOutputCopyFileOpt ] Check Pure CID : %s", strCid ) );
			
			db = new DbTransaction();
			conn = db.startAutoCommit();
			
			try (PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM content WHERE cid=?")) {				
				pstmt.setString(1, strPureCid);
				try( ResultSet rs = pstmt.executeQuery()){
					if( rs.next() ){
						strEncOutputPath = rs.getString( "enc_output_path" );
						strEncOutputPathReal = rs.getString( "enc_real_output_path" );	
						strFramesize = rs.getString("framesize");
						strType = rs.getString("type");
						strPossnYn = rs.getString("possn_yn");
						strEqualHlsCid = rs.getString("equal_hls_cid");
					}
				}				
			}catch( Exception ex ) {
				logger.error("",ex);
				throw ex;
			}finally {
				db.close();
			}
			
			if( strEqualHlsCid == null || strEqualHlsCid.isEmpty() ) {
				throw new Exception( String.format("Get Failure Equal HLS CID | [ RTSP CID : %s ]", strPureCid ) );
			}
			
			if( strEncOutputPath == null || strEncOutputPath.isEmpty() ) {
				throw new Exception( String.format( "Get Failure Pure E-CDN CID Enc Output Path | [ Pure CID : %s ] [ Param CID : %s ]", strPureCid, strCid ) );
			}
			
			if( strEncOutputPathReal == null || strEncOutputPathReal.isEmpty() ) {
				throw new Exception( String.format( "Get Failure Pure E-CDN CID Enc Output Path Real | [ Pure CID : %s ] [ Param CID : %s ]", strPureCid, strCid ) );
			}
			
			if( strFramesize == null || strFramesize.isEmpty() ) {
				throw new Exception( String.format( "Get Failure Pure E-CDN CID Framesize | [ Pure CID : %s ] [ Param CID : %s ]", strPureCid, strCid ) );
			}
			
			if( strType == null || strType.isEmpty() ) {
				throw new Exception( String.format( "Get Failure Pure E-CDN CID Type | [ Pure CID : %s ] [ Param CID : %s ]", strPureCid, strCid ) );
			}
			
			if( strPossnYn == null || strPossnYn.isEmpty() ) {
				throw new Exception( String.format( "Get Failure Pure E-CDN CID PossnYn | [ Pure CID : %s ] [ Param CID : %s ]", strPureCid, strCid ) );
			}
			
			//OutputPath의 To Path File 명은 From Path의 Profile 부분을 삭제하고 _TEMP 를 붙이도록 하자 
			//후에  MSPS에서 MP4 Convet 후  MP4 Clean File을 가지고 온 후 삭제해야 하기 때뭉에... 			
			String StrAddPart = "";
			if( strType.toUpperCase().equals("TV") ) {
				StrAddPart = String.format("_[TEMP_TV%s%s]", strFramesize.toUpperCase(), strPossnYn.toUpperCase() );
			}else {
				StrAddPart = String.format("_[TEMP_MO%s%s]", strFramesize.toUpperCase(), strPossnYn.toUpperCase() );
			}
						
			String strOutputFileNamePure = Util.GetPureFileName( Paths.get( strEncOutputPath ).getFileName().toString() );
			String strOutputFileNameExt = Util.GetExt( Paths.get( strEncOutputPath ).getFileName().toString() );
			String strOutputFileNameRemoveProfilePart = strOutputFileNamePure.substring(0, strOutputFileNamePure.lastIndexOf("_") );
			String strOutputFileNameResultConv = Paths.get( strEncOutputPath ).getParent().resolve( strOutputFileNameRemoveProfilePart + StrAddPart + strOutputFileNameExt ).toString();
			
			logger.info( String.format("[ SetResultEcdnEncOutputCopyFileInfo ] Conv Result Enc Output Path : %s | [ HLS CID : %s ]", strOutputFileNameResultConv, strEqualHlsCid ) );
			
			DbTransaction.updateDbSql( String.format("update cems.content_hls_enc set enc_output_path='%s', enc_real_output_path='%s' where cid='%s'"
					, strOutputFileNameResultConv
					, strEncOutputPathReal
					, strEqualHlsCid 
					) );
			
			bRet = true;
			
		}catch( Exception ex ) {
			logger.error("",ex);
			throw ex;
		}
		
		return bRet;
	}
	
	// 가상CID 스케줄을 Enc Output Copy File로 처리할 경우만 적용가능 로직 만약 시나리오가 변경될 경우 수정필요 | 19.01.31
	public static int IsEncCopyOutFileTaskCnt( Connection paramConn, String strCid ) throws Exception
	{
		int nRet = 0;
		
		DbTransaction db = null;
		Connection conn = null;
		
		try {
			if( strCid == null || strCid.isEmpty() ) {
				throw new Exception( "Param CID is Empty" );
			}
			
			if( paramConn == null ) {
				db = new DbTransaction();
				conn = db.startAutoCommit();	
			}else {
				conn = paramConn;
			}
			
			try (PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM content WHERE ( hls_yn = null or hls_yn = '' ) and cid =?")) {				
				pstmt.setString(1, strCid);				
				try( ResultSet rs = pstmt.executeQuery()){
					while( rs.next() ){
						String strEqulHlsCid = rs.getString("equal_hls_cid");
						if( strEqulHlsCid != null && !strEqulHlsCid.isEmpty() ) {
							++nRet;
						}							
					}
				}				
			}catch( Exception ex ) {
				logger.error("",ex);
				throw ex;
			}finally {
				if( paramConn == null ) {
					db.close();	
				}				
			}
			
			
		}catch( Exception ex ) {
			logger.error("",ex);
			throw ex;
		}
		
		return nRet;
	}
	
	protected class ProcEncodingResult implements Runnable
	{
		private String cid = null;
		private JSONObject jsn = null;
		private FileIoHandler ioHandler = null;
		
		
		public ProcEncodingResult( FileIoHandler ioHandler, String cid, JSONObject jsn )
		{
			this.ioHandler = ioHandler;
			this.cid = cid;
			this.jsn = jsn;
		}		
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			try {
				Proc( ioHandler, cid, jsn );	
			}catch( Exception e ) {
				logger.error("",e);
			}					
		}
		
//		private void Proc( FileIoHandler ioHandler, String cid, JSONObject jsn ) throws Exception  
//		{
//			long error = 0;
//			long sid = 0;
//			String filepath = null;
//			String realFilepath = null;
//			String inputpath = null;
//			boolean bAllTaskComplte = true;
//			
//			ArrayList<EncCompleteItem> listRet = new ArrayList<>();
//			SystemItem system = null;
//
//			try {
//				error = (long) JsonValue.get(jsn, "error");
//				sid = Long.parseLong(JsonValue.getString(jsn, "sid"));
//				filepath = JsonValue.getString(jsn, "filepath");
//				realFilepath = JsonValue.getString(jsn, "encoder_output_filepath");		
//				inputpath = JsonValue.getString(jsn, "inputpath");
//				//"inputpath" : "/mnt/data/EMS/in/DS_302996_7D(영어자막)_5_[TV-HD]_161227.ts",
//				
//				// 트랜스코더 큐에 장비ID를 반환한다.
//				Long pid = null;
//				if( Util.IdTypeIsRTSP( cid ) ) {
//					pid = ProfileAssignInfo.getProfileID("content", cid );	
//				}else {
//					pid = ProfileAssignInfo.getProfileID("content_hls_enc", cid );
//				}
//				
//				if(pid != null){
//					ProfileAssignInfo.ProfileInfo pInfo = ProfileAssignInfo.getProfileType(pid);
//					int assign = ProfileAssignInfo.getAssign(pInfo.type, pInfo.resolution, pInfo.strDivision );
//					if( assign != -1 ){
//						
////						int nEncCopyTaskCnt = IsEncCopyOutFileTaskCnt( null, cid );
////						if( nEncCopyTaskCnt > 0 ) {
////							assign = ( assign * ( nEncCopyTaskCnt + 1 ) );
////						}
//						
//						LoadBalancerEncoderElemental.returnIdle(sid, assign);
//						int nAssignCur = LoadBalancerEncoderElemental.GetAssignCur( null, sid );
//						logger.info("=======================================================================");
//						logger.info(String.format("Enc Assign Return OK | [ CID : %s ] [ Type : %s ] [ Framesize : %s ] [ SID : %d ] [ Return Assign : %d ] [ Cur Assign : %d ]",
//								cid,								
//								pInfo.type,
//								pInfo.resolution,
//								sid, 
//								assign, 
//								nAssignCur 
//								));
//						logger.info("=======================================================================");
//					} else {
//						logger.error("[{}] ElementalEncoderLoadBalancer.returnIdle()", sid);
//					}
//				}else{
//					throw new EmsException(ErrorCode.INVALID_VALUE_PROPERTY, String.format("result pid is not exist | [tbl : moblie ] [ cid : %s ]", cid));
//				}
//
//				if (error != 0) {
//					throw new EmsException(ErrorCode.API_RESPONSE_ERROR, JsonValue.getString(jsn, "desc", true ) + "/" + JsonValue.getString(jsn, "encoder_err", true ));
//				}
//				
//				
//				// 1. TransAgent에서 넘어온 Enc Output File Path 기본정보를 업데이트 한다.
//				if( Util.IdTypeIsRTSP( cid ) ) {
//					DbTransaction.updateDbSql( String.format("update cems.content set enc_output_path='%s', enc_real_output_path='%s' where cid='%s'"
//							, filepath
//							, realFilepath
//							, cid 
//							) );	
//				}else {
//					DbTransaction.updateDbSql( String.format("update cems.content_hls_enc set enc_output_path='%s', enc_real_output_path='%s' where cid='%s'"
//							, filepath
//							, realFilepath
//							, cid 
//							) );
//				}
//				
//				if( !RemarkedEncCompleteEx( cid, filepath, realFilepath ) ){					
//					bAllTaskComplte = false;
//				}
//				
//				if (error != 0) {
//					throw new EmsException(ErrorCode.API_RESPONSE_ERROR, JsonValue.getString(jsn, "desc") + "/" + JsonValue.getString(jsn, "encoder_err"));
//				}
//				
//				if( !ContinueEncTask( cid ) ){
//					logger.info( String.format("Already Relation CID is Task Failure | [ cid : %s ]", cid ) );
//					return;
//				}
//				
//				if( !bAllTaskComplte ){
//					logger.info( String.format("Reamind Task Same Quality... | [ Cid : %s ] ", cid ) );
//					return;
//				}
//				
//				// 2. 	관련된 CID의 인코딩작업이 완료 되었을 경우 E-CDN Enc Output CopyFile 옵션이 있는 작업인지, 아니면  TS + MP4  작업인지 확인
//				//		만약 Enc Output CopyFile 옵션과 관련된 작업이면 관련 가상CID에 복사할 정보 입력
//				if( IsExistEcdnEncOutputCopyFileOptEx( cid ) ) {
//					SetResultEcdnEncOutputCopyFileInfoEx( cid );
//				}
//				
//				listRet = GetRelationEncOutputInfoEx( cid );
//				if( listRet == null || listRet.size() <= 0 ){
//					throw new Exception( String.format("Get Failure RelationInfo | [ Cid : %s ]", cid) );
//				}				
//				
//				for( EncCompleteItem tempEncCompleteItem : listRet ){
//					String filename = Paths.get(tempEncCompleteItem.strEncOutputFilePath).getFileName().toString();
//					String filenameWoExt = filename.substring(0, filename.lastIndexOf('.'));
//					
//					if( tempEncCompleteItem.eIdType == Define.IDType.RTSP ) {
//						DbTransaction.updateDbSql("UPDATE content SET last_time=now(3), sid=null, content_status='작업진행', status='[원본]인코딩파일이동', mapped_filename='" + filenameWoExt +"' WHERE cid='" + tempEncCompleteItem.strCid + "'");
//						DbHelper.insertContentHistroy("cid='" + tempEncCompleteItem.strCid + "'");	
//					}else if( tempEncCompleteItem.eIdType == Define.IDType.HLS ) {
//						DbTransaction.updateDbSql("UPDATE content_hls_enc SET last_time=now(3), sid=null, status='[원본]인코딩파일이동', mapped_filename='" + filenameWoExt +"' WHERE cid='" + tempEncCompleteItem.strCid + "'");
//						DbHelper.insertContentHistroyHls(null, tempEncCompleteItem.strCid );
//					}					
//				}
//				
//				TranscodingStatus.signal();
//				TranscodingTask.signal();
//				BroadcastTaskManager.signal();
//				
//				system = LoadBalancerEncoderElemental.getSystemTbl().get(sid);
//				
//				ArrayList<Util.mountInfo> mountInfoList = MountInfo.Get();
//				if( mountInfoList.size() <= 0 ){
//					throw new EmsException(ErrorCode.MOUNTINFO_IS_NULL, "mount info is null" );
//				}
//				
//				boolean bAllClear = true;
//				String strFailureCid = "";
//				String strFailureFromPath = "";
//				String strFailureToPath = "";				
//				String strPertitleMediaFanURL = new DbCommonHelper().GetConfigureDBValue(null, Define.DBConfigureKey.pertitle_url_mediafan.toString() );
//				String strPertitleStoreRootPathRaw = new DbCommonHelper().GetConfigureDBValue(null, Define.DBConfigureKey.pertitle_store_root_path_raw.toString() );
//				String strPertitleStoreRootPathTrans = new DbCommonHelper().GetConfigureDBValue(null, Define.DBConfigureKey.pertitle_store_root_path_trans.toString() );
//				String strContStgIp = new DbCommonHelper().GetConfigureDBValue(null, Define.DBConfigureKey.content_task_storage_ip.toString() );
//				
//				for( EncCompleteItem tempEncCompleteItem : listRet ){
//					
//					logger.info( String.format("Enc Complete Netc Proc File Copy Start | [ %s ]",  tempEncCompleteItem.strCid ) );
//					
//					boolean bUsagePertitle = false;
//					String strFilenameTrans = Paths.get(tempEncCompleteItem.strEncOutputFilePath).getFileName().toString();
//					String strFrom = null;
//					String strTo = null;
//					String strToConv = null;					
//					String strFromConvRaw = null;
//					String strToConvRaw = null;
//					
//					if( tempEncCompleteItem.encd_piqu_typ_cd == Define.EncdPiquTypCd.Normal.get() ) {
//						strFrom = "\\\\" + system.ip + tempEncCompleteItem.strEncRealOutputFilePath.substring(4).replace("/", "\\");
//						strTo = Paths.get(EmsPropertiesReader.get("[TS]시작.io.dest")).resolve(strFilenameTrans).toString();
//						strToConv = Util.ConverteToNWDriveIP(strTo, mountInfoList);
//					}else if( tempEncCompleteItem.encd_piqu_typ_cd == Define.EncdPiquTypCd.Supernova.get() ) {
//						strFrom = "\\\\" + system.ip + tempEncCompleteItem.strEncRealOutputFilePath.replace("/", "\\");
//						strTo = Paths.get(EmsPropertiesReader.get("[TS]시작.io.dest")).resolve(strFilenameTrans).toString();
//						strToConv = Util.ConverteToNWDriveIP(strTo, mountInfoList);
//					}else if( tempEncCompleteItem.encd_piqu_typ_cd == Define.EncdPiquTypCd.ElementalRetry.get() ) {
//						strFrom = "\\\\" + system.ip + tempEncCompleteItem.strEncRealOutputFilePath.substring(4).replace("/", "\\");
//						
//						bUsagePertitle = new DbCommonHelper().isUsagePertitle(null, tempEncCompleteItem.eIdType, tempEncCompleteItem.strFramesize, tempEncCompleteItem.strType );
//						
//						if( bUsagePertitle && ( tempEncCompleteItem.lExtraStatus & ExtraStatusManipulation.Flag.EF_REPLACE_JOB.get() ) != 0 ) {
//							strToConv = strPertitleStoreRootPathTrans + "\\" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + "\\" + strFilenameTrans;
//						}else {
//							strTo = Paths.get(EmsPropertiesReader.get("[TS]시작.io.dest")).resolve(strFilenameTrans).toString();
//							strToConv = Util.ConverteToNWDriveIP(strTo, mountInfoList);
//						}						
//						 						
//					}else {
//						throw new Exception( String.format("정의되지 않은 EncdPiquTypCd 값 입니다 | [ ID : %s ] [ EncdPiquTypCd : %d ]", tempEncCompleteItem.strCid, tempEncCompleteItem.encd_piqu_typ_cd ) );
//					}
//						
//					// 인코딩 파일 복사
//					EMSProcServiceCopyFileIo fileIo = new EMSProcServiceCopyFileIo(strFrom, strToConv);
//					fileIo.setSrcIp(Util.getCallingIP(strContStgIp));
//					fileIo.setServletParam("q=3&cid=" + tempEncCompleteItem.strCid );
//					if( fileIo.start( null ) ){						
//						if( tempEncCompleteItem.eIdType == Define.IDType.RTSP ) {
//							DbTransaction.updateDbSql("UPDATE content SET last_time=now(3) WHERE cid='" + tempEncCompleteItem.strCid + "'");	
//						}else if( tempEncCompleteItem.eIdType == Define.IDType.HLS ) {
//							DbTransaction.updateDbSql("UPDATE content_hls_enc SET last_time=now(3) WHERE cid='" + tempEncCompleteItem.strCid + "'");
//						}						
//					}else{						
//						bAllClear = false;
//						strFailureCid = tempEncCompleteItem.strCid;
//						strFailureFromPath = strFrom;
//						strFailureToPath = strToConv;											
//					}		
//					
//					// 인코딩 원본 파일 복사 & MediaFan REPLACE 요청
//					if( tempEncCompleteItem.encd_piqu_typ_cd == Define.EncdPiquTypCd.ElementalRetry.get() && 
//						( tempEncCompleteItem.lExtraStatus & ExtraStatusManipulation.Flag.EF_REPLACE_JOB.get() ) != 0 && 
//						bUsagePertitle 
//						) 
//						 
//					{
//						strFromConvRaw = Util.ConverteToNWDriveIP(tempEncCompleteItem.str1stFilePath, mountInfoList);
//						strToConvRaw = strPertitleStoreRootPathRaw + "\\" + tempEncCompleteItem.eIdType.toString() + "\\" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + "\\" + String.format("%s_%s", Util.ClearReplaceTag( tempEncCompleteItem.strCid ).replace("{", "").replace("}", ""), Paths.get( tempEncCompleteItem.str1stFilePath ).getFileName().toString() );
//											
//						fileIo = new EMSProcServiceCopyFileIo(strFromConvRaw, strToConvRaw);
//						fileIo.setSrcIp(Util.getCallingIP(strContStgIp));
//						fileIo.setServletParam("q=3&cid=" + tempEncCompleteItem.strCid );
//						if( !fileIo.start( null ) ){						
//							throw new Exception( String.format("원본파일을 화질 퍼타이틀 NAS에 복사하는데 실패 하였습니다. | [ %s ] [ From : %s ] >>> [ To : %s ]",
//									tempEncCompleteItem.strCid,
//									strFromConvRaw,
//									strToConvRaw
//									) );						
//						}
//												
//						
//						if( tempEncCompleteItem.eIdType == Define.IDType.RTSP ) {
//							
//							RequestPertitleMediaFanAGT reqMediaFan = new RequestPertitleMediaFanAGT.Builder(Util.ClearReplaceTag( tempEncCompleteItem.strCid ), tempEncCompleteItem.eIdType, strPertitleMediaFanURL, null).setBitrateCode(tempEncCompleteItem.rcmd_brt_typ_cd)
//																																													.setCaptionUsage( tempEncCompleteItem.nSubUsage == 1 ? true : false )
//																																													.setIFType(IFType.REPLACE)
//																																													.setEpsdID(Util.ClearReplaceTag( tempEncCompleteItem.strEpsdID ) )
//																																													.setMdaRsluID(tempEncCompleteItem.listMdaRsluId)
//																																													.setPathRaw(strToConvRaw)
//																																													.setPathTrans(strToConv)
//																																													.build();
//
//							if( !reqMediaFan.action(null) ) {
//								throw new Exception( String.format("퍼타이틀 MediaFaan 화질교체 작업요청에 실패하였습니다 | [ %s ] [ %s ]", tempEncCompleteItem.strCid, tempEncCompleteItem.eIdType ) );
//							}
//							
//							DbTransaction.updateDbSql( String.format( "UPDATE content SET status='%s', 1st_path_store_pertitle='%s', 2nd_path_store_pertitle='%s' WHERE cid='%s'", 
//																		Define.strStatus_Tasking_Raw_Vmaf + "(0%)", 
//																		strToConvRaw.replace("\\", "\\\\"),
//																		strToConv.replace("\\", "\\\\"),
//																		tempEncCompleteItem.strCid 
//																		));	
//						}else if( tempEncCompleteItem.eIdType == Define.IDType.HLS ) {
//							
//							String strMdaRsluID = new DbCommonHelper().matchHLSMdaRsluID(null, tempEncCompleteItem.strHlsMediaId, tempEncCompleteItem.strFramesize );
//							if( Checker.isEmpty( strMdaRsluID ) ) {
//								throw new Exception( String.format("HLS 미디어 해상도 ID를 조회하는데 실패하였습니다 | [ MdaID : %s ] [ Framesize : %s ]", tempEncCompleteItem.strHlsMediaId, tempEncCompleteItem.strFramesize ) );
//							}
//							
//							RequestPertitleMediaFanAGT reqMediaFan = new RequestPertitleMediaFanAGT.Builder(strMdaRsluID, tempEncCompleteItem.eIdType, strPertitleMediaFanURL, null).setBitrateCode(tempEncCompleteItem.rcmd_brt_typ_cd)
//																																													.setCaptionUsage(false)
//																																													.setIFType(IFType.REPLACE)
//																																													.setPathRaw(strToConvRaw)
//																																													.setPathTrans(strToConv)
//																																													.build();
//							
//							if( !reqMediaFan.action(null) ) {
//								throw new Exception( String.format("퍼타이틀 MediaFaan 화질교체 작업요청에 실패하였습니다 | [ %s ] [ %s ]", tempEncCompleteItem.strCid, tempEncCompleteItem.eIdType ) );
//							}
//							
//							DbTransaction.updateDbSql( String.format("UPDATE content_hls_enc SET status='%s', 1st_path_store_pertitle='%s', 2nd_path_store_pertitle='%s' WHERE cid='%s'", 
//																	Define.strStatus_Tasking_Raw_Vmaf + "(0%)",
//																	strToConvRaw.replace("\\", "\\\\"),
//																	strToConv.replace("\\", "\\\\"),
//																	tempEncCompleteItem.strCid  
//																	) );
//						}
//						
//						ObservingQVAL.collect();						
//					}
//				}
//				
//				if( !bAllClear ){
//					DeleteEncOutputFiles( listRet, system, mountInfoList );					
//					throw new Exception( String.format("Enc Out -> TS Start Dir File Copy Failure | [ cid : %s ] [ from path : %s ] >>>> [ to path : %s ]"
//							, strFailureCid
//							, strFailureFromPath
//							, strFailureToPath
//							) );
//				}
//				
//				TaskReqReplaceIP ctlReqIp = new TaskReqReplaceIP();
//				ArrayList<TaskReqReplaceIP.RpIPItem> listTaskRpIP = ctlReqIp.GetReplaceIPList(null);
//				
//				// 복사가 완료되면 호크아이 작업 진행
//				for( EncCompleteItem tempEncCompleteItem : listRet ) {					
//					String filename = Paths.get(tempEncCompleteItem.strEncOutputFilePath).getFileName().toString();
//					Path dest = Paths.get(EmsPropertiesReader.get("[TS]시작.io.dest"));
//					dest = dest.resolve(Paths.get(tempEncCompleteItem.strEncOutputFilePath).getFileName().toString());
//					String from = null;
//					if( tempEncCompleteItem.encd_piqu_typ_cd == Define.EncdPiquTypCd.Supernova.get() ) {
//						from = "\\\\" + system.ip + tempEncCompleteItem.strEncRealOutputFilePath.replace("/", "\\");
//					}else{
//						from = "\\\\" + system.ip + tempEncCompleteItem.strEncRealOutputFilePath.substring(4).replace("/", "\\");	
//					}
//					
//					String to = Util.ConverteToNWDriveIP(dest.toString(), mountInfoList);					
//					
//					if( ( tempEncCompleteItem.encd_piqu_typ_cd == Define.EncdPiquTypCd.Normal.get() || tempEncCompleteItem.encd_piqu_typ_cd == Define.EncdPiquTypCd.ElementalRetry.get() ) && 
//						HawkEyeManager.IsUse() && 
//						tempEncCompleteItem.strType != null && 
//						tempEncCompleteItem.strType.toUpperCase().equals("TV") && 
//						tempEncCompleteItem.eIdType == Define.IDType.RTSP )
//					{	
//						logger.info(String.format("HawkeEye Start!!! | [ cid : %s ] [ Path : %s ]", tempEncCompleteItem.strCid, dest ));
//						String strEpsdID = GetEpsdID( tempEncCompleteItem.strCid );							
//						HawkEyeManager.HawkEyeFTPInfo info = HawkEyeManager.GetFTPInfo();
//						String ftpUrl = String.format("ftp://%s:%s/%s/%s", info.ip, info.port, new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime()), filename );
//						String convElementalFilePath = HawkEyeManager.ConvElementalLocalPath(from);
//						String callIp = Util.getCallingIP(to);
//						HawkEyeFIleUploader he = new HawkEyeFIleUploader(																		
//																		strEpsdID == null ? "" : strEpsdID,
//																		tempEncCompleteItem.strCid, 
//																		system.userid, 		// 엘리멘탈 ssh ID
//																		system.userpw, 		// 엘리멘탈 ssh PW
//																		ctlReqIp.ChkIPReplace( system.ip, listTaskRpIP ), 			// 엘리멘탈 ssh IP
//																		"22", 				// 엘리멘탈 ssh port	
//																		info.id, 			// HawkEye File Upload FTP Svr ID
//																		info.pw, 		// HawkEye File Upload FTP Svr PW 
//																		convElementalFilePath,	// 엘리멘탈 로컬 경로 
//																		ftpUrl, 	// HawkEye File Upload FTP Svr Path
//																		from, 
//																		callIp,
//																		sid,
//																		system.ip
//																		);
//						he.start();
//					}else {
//						EMSProcServiceDeleteFileIo delIo = new EMSProcServiceDeleteFileIo(from, null);			
//						delIo.setSrcIp(Util.getCallingIP(to));
//						delIo.start(null);							
//					}
//				}
//				
//				// 
//
//			} catch (Exception e) {
//				logger.error("",e);
//				ArrayList<Define.IDItem> listCids = Util.GetRelationCids( cid );
//				for( Define.IDItem idItem : listCids ){
//					if( idItem.eIDType == Define.IDType.RTSP ) {
//						DbTransaction.updateDbSql("UPDATE content SET last_time=now(3), content_status='작업오류', status='[원본]인코딩실패', note='" + Util.CuttingMaximumStr( e.getMessage(), 500 ) + "' WHERE cid='" + idItem.strID + "'");
//						DbHelper.insertContentHistroy("cid='" + idItem.strID + "'");
//						new ReportNCMS().ReportEpsdResolutionInfo(idItem.strID, ReportNCMS.STATUS_CODE.ErrorTask );	
//					}else if( idItem.eIDType == Define.IDType.HLS ) {
//						DbTransaction.updateDbSql("UPDATE content_hls_enc a left outer join content_hls b on a.mda_id = b.mda_id SET a.last_time=now(3), a.status='[원본]인코딩실패', a.note='" + Util.CuttingMaximumStr( e.getMessage(), 500 ) + "' WHERE a.cid='" + idItem.strID + "'");
//						DbHelper.insertContentHistroyHls(null, idItem.strID );
//					}					
//				}
//
//				ArrayList<String> listUpdateMdaId = new ArrayList<>();
//				for( Define.IDItem idItem : listCids ){
//					if( idItem.eIDType == Define.IDType.HLS && !listUpdateMdaId.contains( idItem.strHlsMediaID ) ) {
//						
//						boolean bLastError = false;
//										
//						if( DbCommonHelper.IsHlsLastErrorCheckItem( null, idItem.strHlsMediaID, idItem.strID ) ) {
//							bLastError = true;	
//						}														
//								
//						if( bLastError ) {
//							DbTransaction.updateDbSql( String.format( "UPDATE content_hls SET mda_matl_sts_cd='작업오류' WHERE mda_id='%s'", idItem.strHlsMediaID ) );
//							DbTransaction.updateDbSql(String.format("UPDATE content set content_status='작업오류', status='작업오류' WHERE cid='%s'", idItem.strHlsMediaID ));
//							new ReportNCMSHls().mediaEncodingResult( null, idItem.strHlsMediaID, ReportNCMS.STATUS_CODE.ErrorTask );	
//						}else {
//							logger.info( String.format( String.format("Is Not Last Error Hls Sche, Skip Task Error Matl Sts Cd Setting | [ %s ]", idItem.strHlsMediaID) ) );
//						}
//						
//						DbTransaction.updateDbSql("UPDATE content SET last_time=now(3), content_status='작업오류', status='작업오류' WHERE cid='" + idItem.strHlsMediaID + "'");
//						listUpdateMdaId.add( idItem.strHlsMediaID );
//					}					
//				}
//				
//				throw e;
//			} finally {
//				
//				String strTaskStorageIp = new DbCommonHelper().GetConfigureDBValue(null, "content_task_storage_ip");
//				if( system != null && strTaskStorageIp != null && !strTaskStorageIp.isEmpty() ) {					
//					for( EncCompleteItem tempEncCompleteItem : listRet ){				
//						// 원본 파일 삭제
//						removeInputFileEx(tempEncCompleteItem.strCid, system, inputpath, strTaskStorageIp );
//					}	
//				}				
//				
//				TranscodingStatus.signal();
//				TranscodingTask.signal();
//				BroadcastTaskManager.signal();
//			}
//		}	
		
		
		private void Proc( FileIoHandler ioHandler, String cid, JSONObject jsn ) throws Exception  
		{
			long error = 0;
			long sid = 0;
			String filepath = null;
			String realFilepath = null;
			String inputpath = null;
			boolean bAllTaskComplte = true;
			
			ArrayList<EncCompleteItem> listRet = new ArrayList<>();
			SystemItem system = null;

			try {
				error = (long) JsonValue.get(jsn, "error");
				sid = Long.parseLong(JsonValue.getString(jsn, "sid"));
				filepath = JsonValue.getString(jsn, "filepath");
				realFilepath = JsonValue.getString(jsn, "encoder_output_filepath");		
				inputpath = JsonValue.getString(jsn, "inputpath");
				//"inputpath" : "/mnt/data/EMS/in/DS_302996_7D(영어자막)_5_[TV-HD]_161227.ts",
				
				// 트랜스코더 큐에 장비ID를 반환한다.
				Long pid = null;
				if( Util.IdTypeIsRTSP( cid ) ) {
					pid = ProfileAssignInfo.getProfileID("content", cid );	
				}else {
					pid = ProfileAssignInfo.getProfileID("content_hls_enc", cid );
				}
				
				if(pid != null){
					ProfileAssignInfo.ProfileInfo pInfo = ProfileAssignInfo.getProfileType(pid);
//					int assign = ProfileAssignInfo.getAssign(pInfo.type, pInfo.resolution, pInfo.strDivision );
					int assign = ProfileAssignInfo.getAssign(pInfo.type, pInfo.resolution, pInfo.strDivision, sid );
					logger.info(String.format("==============Return Idle assign [%s]============", assign));

					if( assign != -1 ){
						
//						int nEncCopyTaskCnt = IsEncCopyOutFileTaskCnt( null, cid );
//						if( nEncCopyTaskCnt > 0 ) {
//							assign = ( assign * ( nEncCopyTaskCnt + 1 ) );
//						}
						
						LoadBalancerEncoderElemental.returnIdle(sid, assign);
						int nAssignCur = LoadBalancerEncoderElemental.GetAssignCur( null, sid );
						logger.info("=======================================================================");
						logger.info(String.format("Enc Assign Return OK | [ CID : %s ] [ Type : %s ] [ Framesize : %s ] [ SID : %d ] [ Return Assign : %d ] [ Cur Assign : %d ]",
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
					throw new EmsException(ErrorCode.API_RESPONSE_ERROR, JsonValue.getString(jsn, "desc", true ) + "/" + JsonValue.getString(jsn, "encoder_err", true ));
				}
				
				
				// 1. TransAgent에서 넘어온 Enc Output File Path 기본정보를 업데이트 한다.
				if( Util.IdTypeIsRTSP( cid ) ) {
					DbTransaction.updateDbSql( String.format("update cems.content set enc_output_path='%s', enc_real_output_path='%s' where cid='%s'"
							, filepath
							, realFilepath
							, cid 
							) );	
				}else {
					DbTransaction.updateDbSql( String.format("update cems.content_hls_enc set enc_output_path='%s', enc_real_output_path='%s' where cid='%s'"
							, filepath
							, realFilepath
							, cid 
							) );
				}
				
				if( !RemarkedEncCompleteEx( cid, filepath, realFilepath ) ){					
					bAllTaskComplte = false;
				}
				
				if (error != 0) {
					throw new EmsException(ErrorCode.API_RESPONSE_ERROR, JsonValue.getString(jsn, "desc") + "/" + JsonValue.getString(jsn, "encoder_err"));
				}
				
				if( !ContinueEncTask( cid ) ){
					logger.info( String.format("Already Relation CID is Task Failure | [ cid : %s ]", cid ) );
					return;
				}
				
				if( !bAllTaskComplte ){
					logger.info( String.format("Reamind Task Same Quality... | [ Cid : %s ] ", cid ) );
					return;
				}
				
				// 2. 	관련된 CID의 인코딩작업이 완료 되었을 경우 E-CDN Enc Output CopyFile 옵션이 있는 작업인지, 아니면  TS + MP4  작업인지 확인
				//		만약 Enc Output CopyFile 옵션과 관련된 작업이면 관련 가상CID에 복사할 정보 입력
				if( IsExistEcdnEncOutputCopyFileOptEx( cid ) ) {
					SetResultEcdnEncOutputCopyFileInfoEx( cid );
				}
				
				listRet = GetRelationEncOutputInfoEx( cid );
				if( listRet == null || listRet.size() <= 0 ){
					throw new Exception( String.format("Get Failure RelationInfo | [ Cid : %s ]", cid) );
				}				
				
				for( EncCompleteItem tempEncCompleteItem : listRet ){
					String filename = Paths.get(tempEncCompleteItem.strEncOutputFilePath).getFileName().toString();
					String filenameWoExt = filename.substring(0, filename.lastIndexOf('.'));
					
					if( tempEncCompleteItem.eIdType == Define.IDType.RTSP ) {
						DbTransaction.updateDbSql("UPDATE content SET last_time=now(3), sid=null, content_status='작업진행', status='[원본]인코딩파일이동', mapped_filename='" + filenameWoExt +"' WHERE cid='" + tempEncCompleteItem.strCid + "'");
						DbHelper.insertContentHistroy("cid='" + tempEncCompleteItem.strCid + "'");	
					}else if( tempEncCompleteItem.eIdType == Define.IDType.HLS ) {
						DbTransaction.updateDbSql("UPDATE content_hls_enc SET last_time=now(3), sid=null, status='[원본]인코딩파일이동', mapped_filename='" + filenameWoExt +"' WHERE cid='" + tempEncCompleteItem.strCid + "'");
						DbHelper.insertContentHistroyHls(null, tempEncCompleteItem.strCid );
					}					
				}
				
				TranscodingStatus.signal();
				TranscodingTask.signal();
				BroadcastTaskManager.signal();
				
				system = LoadBalancerEncoderElemental.getSystemTbl().get(sid);
				
				ArrayList<Util.mountInfo> mountInfoList = MountInfo.Get();
				if( mountInfoList.size() <= 0 ){
					throw new EmsException(ErrorCode.MOUNTINFO_IS_NULL, "mount info is null" );
				}
				
				boolean bAllClear = true;
				String strFailureCid = "";
				String strFailureFromPath = "";
				String strFailureToPath = "";				
				String strPertitleMediaFanURL = new DbCommonHelper().GetConfigureDBValue(null, Define.DBConfigureKey.pertitle_url_mediafan.toString() );
				String strPertitleStoreRootPathRaw = new DbCommonHelper().GetConfigureDBValue(null, Define.DBConfigureKey.pertitle_store_root_path_raw.toString() );
				String strPertitleStoreRootPathTrans = new DbCommonHelper().GetConfigureDBValue(null, Define.DBConfigureKey.pertitle_store_root_path_trans.toString() );
				String strContStgIp = new DbCommonHelper().GetConfigureDBValue(null, Define.DBConfigureKey.content_task_storage_ip.toString() );
				
				for( EncCompleteItem tempEncCompleteItem : listRet ){
					
					logger.info( String.format("Enc Complete Netc Proc File Copy Start | [ %s ]",  tempEncCompleteItem.strCid ) );
					
					boolean bUsagePertitle = false;
					String strFilenameTrans = Paths.get(tempEncCompleteItem.strEncOutputFilePath).getFileName().toString();
					String strFrom = null;
					String strTo = null;
					String strToConv = null;					
					String strFromConvRaw = null;
					String strToConvRaw = null;
					
					if( tempEncCompleteItem.encd_piqu_typ_cd == Define.EncdPiquTypCd.Supernova.get() ) {
						strFrom = "\\\\" + system.ip + tempEncCompleteItem.strEncRealOutputFilePath.replace("/", "\\");
						strTo = Paths.get(EmsPropertiesReader.get("[TS]시작.io.dest")).resolve(strFilenameTrans).toString();
						strToConv = Util.ConverteToNWDriveIP(strTo, mountInfoList);
					}else{
						
						bUsagePertitle = new DbCommonHelper().isUsagePertitle(null, tempEncCompleteItem.eIdType, tempEncCompleteItem.strFramesize, tempEncCompleteItem.strType );
						
						if( bUsagePertitle && 
							( tempEncCompleteItem.lExtraStatus & ExtraStatusManipulation.Flag.EF_REPLACE_JOB.get() ) != 0 && 
							tempEncCompleteItem.encd_qlty_res_cd == Define.EncdQltyResCd.RetryRequet.get() 
							) 
						{
							// 화질교체 퍼타이틀
							strFrom = "\\\\" + system.ip + tempEncCompleteItem.strEncRealOutputFilePath.substring(4).replace("/", "\\");
							strToConv = strPertitleStoreRootPathTrans + "\\REPLACE\\" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + "\\" + strFilenameTrans;
						}else {
							strFrom = "\\\\" + system.ip + tempEncCompleteItem.strEncRealOutputFilePath.substring(4).replace("/", "\\");
							strTo = Paths.get(EmsPropertiesReader.get("[TS]시작.io.dest")).resolve(strFilenameTrans).toString();
							strToConv = Util.ConverteToNWDriveIP(strTo, mountInfoList);	
						}							
					}
						
					// 인코딩 파일 복사
					EMSProcServiceCopyFileIo fileIo = new EMSProcServiceCopyFileIo(strFrom, strToConv);
					fileIo.setSrcIp(Util.getCallingIP(strContStgIp));
					fileIo.setServletParam("q=3&cid=" + tempEncCompleteItem.strCid );
					if( fileIo.start( null ) ){						
						if( tempEncCompleteItem.eIdType == Define.IDType.RTSP ) {
							DbTransaction.updateDbSql("UPDATE content SET last_time=now(3) WHERE cid='" + tempEncCompleteItem.strCid + "'");	
						}else if( tempEncCompleteItem.eIdType == Define.IDType.HLS ) {
							DbTransaction.updateDbSql("UPDATE content_hls_enc SET last_time=now(3) WHERE cid='" + tempEncCompleteItem.strCid + "'");
						}						
					}else{						
						bAllClear = false;
						strFailureCid = tempEncCompleteItem.strCid;
						strFailureFromPath = strFrom;
						strFailureToPath = strToConv;											
					}		
					
					// 인코딩 원본 파일 복사 & MediaFan REPLACE 요청
					if( tempEncCompleteItem.encd_qlty_res_cd == Define.EncdQltyResCd.RetryRequet.get() && 
						( tempEncCompleteItem.lExtraStatus & ExtraStatusManipulation.Flag.EF_REPLACE_JOB.get() ) != 0 && 
						bUsagePertitle 
						) 
						 
					{	
						String strPure1stFileName = Util.GetPureFileName( Paths.get( tempEncCompleteItem.str1stFilePath ).getFileName().toString() );
						String strPossnYnPrefix = Checker.isEmpty( tempEncCompleteItem.strPossnYN ) ? "" : String.format("_%s", tempEncCompleteItem.strPossnYN );
						String strExt = Util.GetExt( Paths.get( tempEncCompleteItem.str1stFilePath ).getFileName().toString() );
						
						strFromConvRaw = Util.ConverteToNWDriveIP(tempEncCompleteItem.str1stFilePath, mountInfoList);
						strToConvRaw = strPertitleStoreRootPathRaw + 
										"\\REPLACE\\" + 
										tempEncCompleteItem.eIdType.toString() + 
										"\\" + 
										new SimpleDateFormat("yyyyMMdd").format(new Date()) + 
										"\\" + 
										String.format("%s%s%s", strPure1stFileName, strPossnYnPrefix, strExt );
						
						if( tempEncCompleteItem.eIdType == Define.IDType.RTSP ) {
							DbTransaction.updateDbSql("UPDATE content SET last_time=now(3), status='[원본]검수원본파일이동' WHERE cid='" + tempEncCompleteItem.strCid + "'");
							DbHelper.insertContentHistroy("cid='" + tempEncCompleteItem.strCid + "'");	
						}else if( tempEncCompleteItem.eIdType == Define.IDType.HLS ) {
							DbTransaction.updateDbSql("UPDATE content_hls_enc SET last_time=now(3), status='[원본]검수원본파일이동' WHERE cid='" + tempEncCompleteItem.strCid + "'");
							DbHelper.insertContentHistroyHls(null, tempEncCompleteItem.strCid );
						}
									
						// UHD, HD를 교체작업 할 경우를 가정해서 UHD가 먼저 작업되고 교체완료를 해버릴경우 VMAF에서 원본파일를 찾을 수 없게되는 상황이 발생할 수 있으므로 복사 후 사용한다.
						fileIo = new EMSProcServiceCopyFileIo(strFromConvRaw, strToConvRaw);
						fileIo.setSrcIp(Util.getCallingIP(strContStgIp));
						fileIo.setServletParam("q=52&cid=" + tempEncCompleteItem.strCid );
						if( !fileIo.start( null ) ){						
							throw new Exception( String.format("원본파일을 화질 퍼타이틀 NAS에 복사하는데 실패 하였습니다. | [ %s ] [ From : %s ] >>> [ To : %s ]",
									tempEncCompleteItem.strCid,
									strFromConvRaw,
									strToConvRaw
									) );						
						}
												
						
						if( tempEncCompleteItem.eIdType == Define.IDType.RTSP ) {
							
							RequestPertitleMediaFanAGT reqMediaFan = new RequestPertitleMediaFanAGT.Builder(Util.ClearReplaceTag( tempEncCompleteItem.strCid ), tempEncCompleteItem.eIdType, strPertitleMediaFanURL, null).setBitrateCode(tempEncCompleteItem.rcmd_brt_typ_cd)
																																													.setCaptionUsage( tempEncCompleteItem.nSubUsage == 1 ? true : false )
																																													.setIFType(IFType.REPLACE)
																																													.setEpsdID(Util.ClearReplaceTag( tempEncCompleteItem.strEpsdID ) )
																																													.setMdaRsluID(tempEncCompleteItem.listMdaRsluId)
																																													.setPathRaw(strToConvRaw)
																																													.setPathTrans(strToConv)
																																													.build();

							if( !reqMediaFan.action(null) ) {
								throw new Exception( String.format("퍼타이틀 MediaFaan 화질교체 작업요청에 실패하였습니다 | [ %s ] [ %s ]", tempEncCompleteItem.strCid, tempEncCompleteItem.eIdType ) );
							}
							
							DbTransaction.updateDbSql( String.format( "UPDATE content SET status='%s', 1st_path_store_pertitle='%s', 2nd_path_store_pertitle='%s' WHERE cid='%s'", 
																		Define.strStatus_Tasking_Raw_Vmaf_Wait, 
																		strToConvRaw.replace("\\", "\\\\"),
																		strToConv.replace("\\", "\\\\"),
																		tempEncCompleteItem.strCid 
																		));	
						}else if( tempEncCompleteItem.eIdType == Define.IDType.HLS ) {
							
							String strMdaRsluID = new DbCommonHelper().matchHLSMdaRsluID(null, tempEncCompleteItem.strHlsMediaId, tempEncCompleteItem.strFramesize );
							if( Checker.isEmpty( strMdaRsluID ) ) {
								throw new Exception( String.format("HLS 미디어 해상도 ID를 조회하는데 실패하였습니다 | [ MdaID : %s ] [ Framesize : %s ]", tempEncCompleteItem.strHlsMediaId, tempEncCompleteItem.strFramesize ) );
							}
							
							RequestPertitleMediaFanAGT reqMediaFan = new RequestPertitleMediaFanAGT.Builder(strMdaRsluID, tempEncCompleteItem.eIdType, strPertitleMediaFanURL, null).setBitrateCode(tempEncCompleteItem.rcmd_brt_typ_cd)
																																													.setCaptionUsage(false)
																																													.setIFType(IFType.REPLACE)
																																													.setPathRaw(strToConvRaw)
																																													.setPathTrans(strToConv)
																																													.build();
							
							if( !reqMediaFan.action(null) ) {
								throw new Exception( String.format("퍼타이틀 MediaFaan 화질교체 작업요청에 실패하였습니다 | [ %s ] [ %s ]", tempEncCompleteItem.strCid, tempEncCompleteItem.eIdType ) );
							}
							
							DbTransaction.updateDbSql( String.format("UPDATE content_hls_enc SET status='%s', 1st_path_store_pertitle='%s', 2nd_path_store_pertitle='%s' WHERE cid='%s'", 
																	Define.strStatus_Tasking_Raw_Vmaf_Wait,
																	strToConvRaw.replace("\\", "\\\\"),
																	strToConv.replace("\\", "\\\\"),
																	tempEncCompleteItem.strCid  
																	) );
						}
						
						ObservingQVAL.collect();						
					}
				}
				
				if( !bAllClear ){
					DeleteEncOutputFiles( listRet, system, mountInfoList );					
					throw new Exception( String.format("Enc Out -> TS Start Dir File Copy Failure | [ cid : %s ] [ from path : %s ] >>>> [ to path : %s ]"
							, strFailureCid
							, strFailureFromPath
							, strFailureToPath
							) );
				}
				
				//TaskReqReplaceIP ctlReqIp = new TaskReqReplaceIP();
				ConvIP ctlReqIp = new ConvIP();
				//ArrayList<TaskReqReplaceIP.RpIPItem> listTaskRpIP = ctlReqIp.GetReplaceIPList(null);
				ArrayList<String[]> listTaskRpIP = ctlReqIp.getListConvIP( null, ConvIPFlag.elemental, ConvIPReplaceFlag.c_class );
				
				// 복사가 완료되면 호크아이 작업 진행
				for( EncCompleteItem tempEncCompleteItem : listRet ) {					
					String filename = Paths.get(tempEncCompleteItem.strEncOutputFilePath).getFileName().toString();
					Path dest = Paths.get(EmsPropertiesReader.get("[TS]시작.io.dest"));
					dest = dest.resolve(Paths.get(tempEncCompleteItem.strEncOutputFilePath).getFileName().toString());
					String from = null;
					if( tempEncCompleteItem.encd_piqu_typ_cd == Define.EncdPiquTypCd.Supernova.get() ) {
						from = "\\\\" + system.ip + tempEncCompleteItem.strEncRealOutputFilePath.replace("/", "\\");
					}else{
						from = "\\\\" + system.ip + tempEncCompleteItem.strEncRealOutputFilePath.substring(4).replace("/", "\\");	
					}
					
					String to = Util.ConverteToNWDriveIP(dest.toString(), mountInfoList);					
					
					if( ( tempEncCompleteItem.encd_piqu_typ_cd == Define.EncdPiquTypCd.Normal.get() && tempEncCompleteItem.encd_qlty_res_cd == 0 ) && 
						HawkEyeManager.IsUse() && 
						tempEncCompleteItem.strType != null && 
						tempEncCompleteItem.strType.toUpperCase().equals("TV") && 
						tempEncCompleteItem.eIdType == Define.IDType.RTSP )
					{	
						logger.info(String.format("HawkeEye Start!!! | [ cid : %s ] [ Path : %s ]", tempEncCompleteItem.strCid, dest ));
						String strEpsdID = GetEpsdID( tempEncCompleteItem.strCid );							
						HawkEyeManager.HawkEyeFTPInfo info = HawkEyeManager.GetFTPInfo();
						String ftpUrl = String.format("ftp://%s:%s/%s/%s", info.ip, info.port, new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime()), filename );
						String convElementalFilePath = HawkEyeManager.ConvElementalLocalPath(from);
						String callIp = Util.getCallingIP(to);
						HawkEyeFIleUploader he = new HawkEyeFIleUploader(																		
																		strEpsdID == null ? "" : strEpsdID,
																		tempEncCompleteItem.strCid, 
																		system.userid, 		// 엘리멘탈 ssh ID
																		system.userpw, 		// 엘리멘탈 ssh PW
																		//ctlReqIp.ChkIPReplace( system.ip, listTaskRpIP ), 			// 엘리멘탈 ssh IP
																		ctlReqIp.inquiryConvIP(ConvIPReplaceFlag.c_class, listTaskRpIP, system.ip), // 엘리멘탈 ssh IP
																		"22", 				// 엘리멘탈 ssh port	
																		info.id, 			// HawkEye File Upload FTP Svr ID
																		info.pw, 		// HawkEye File Upload FTP Svr PW 
																		convElementalFilePath,	// 엘리멘탈 로컬 경로 
																		ftpUrl, 	// HawkEye File Upload FTP Svr Path
																		from, 
																		callIp,
																		sid,
																		system.ip
																		);
						he.start();
					}else {
						EMSProcServiceDeleteFileIo delIo = new EMSProcServiceDeleteFileIo(from, null);			
						delIo.setSrcIp(Util.getCallingIP(to));
						delIo.start(null);							
					}
				}
				
				// 

			} catch (Exception e) {
				logger.error("",e);
				ArrayList<Define.IDItem> listCids = Util.GetRelationCids( cid );
				for( Define.IDItem idItem : listCids ){
					if( idItem.eIDType == Define.IDType.RTSP ) {
						DbTransaction.updateDbSql("UPDATE content SET last_time=now(3), content_status='작업오류', status='[원본]인코딩실패', note='" + Util.CuttingMaximumStr( e.getMessage(), 500 ) + "' WHERE cid='" + idItem.strID + "'");
						DbHelper.insertContentHistroy("cid='" + idItem.strID + "'");
						new ReportNCMS().ReportEpsdResolutionInfo(idItem.strID, ReportNCMS.STATUS_CODE.ErrorTask );	
					}else if( idItem.eIDType == Define.IDType.HLS ) {
						DbTransaction.updateDbSql("UPDATE content_hls_enc a left outer join content_hls b on a.mda_id = b.mda_id SET a.last_time=now(3), a.status='[원본]인코딩실패', a.note='" + Util.CuttingMaximumStr( e.getMessage(), 500 ) + "' WHERE a.cid='" + idItem.strID + "'");
						DbHelper.insertContentHistroyHls(null, idItem.strID );
					}					
				}

				ArrayList<String> listUpdateMdaId = new ArrayList<>();
				for( Define.IDItem idItem : listCids ){
					if( idItem.eIDType == Define.IDType.HLS && !listUpdateMdaId.contains( idItem.strHlsMediaID ) ) {
						
						boolean bLastError = false;
										
						if( DbCommonHelper.IsHlsLastErrorCheckItem( null, idItem.strHlsMediaID, idItem.strID ) ) {
							bLastError = true;	
						}														
								
						if( bLastError ) {
							DbTransaction.updateDbSql( String.format( "UPDATE content_hls SET mda_matl_sts_cd='작업오류' WHERE mda_id='%s'", idItem.strHlsMediaID ) );
							DbTransaction.updateDbSql(String.format("UPDATE content set content_status='작업오류', status='작업오류' WHERE cid='%s'", idItem.strHlsMediaID ));
							new ReportNCMSHls().mediaEncodingResult( null, idItem.strHlsMediaID, ReportNCMS.STATUS_CODE.ErrorTask );	
						}else {
							logger.info( String.format( String.format("Is Not Last Error Hls Sche, Skip Task Error Matl Sts Cd Setting | [ %s ]", idItem.strHlsMediaID) ) );
						}
						
						DbTransaction.updateDbSql("UPDATE content SET last_time=now(3), content_status='작업오류', status='작업오류' WHERE cid='" + idItem.strHlsMediaID + "'");
						listUpdateMdaId.add( idItem.strHlsMediaID );
					}					
				}
				
				throw e;
			} finally {
				
				String strTaskStorageIp = new DbCommonHelper().GetConfigureDBValue(null, "content_task_storage_ip");
				if( system != null && strTaskStorageIp != null && !strTaskStorageIp.isEmpty() ) {					
					for( EncCompleteItem tempEncCompleteItem : listRet ){				
						// 원본 파일 삭제
						removeInputFileEx(tempEncCompleteItem.strCid, system, inputpath, strTaskStorageIp );
					}	
				}				
				
				TranscodingStatus.signal();
				TranscodingTask.signal();
				BroadcastTaskManager.signal();
			}
		}
		
		
		private void DeleteEncOutputFiles( ArrayList<EncCompleteItem> listItem, SystemItem system, ArrayList<Util.mountInfo> mountInfoList )
		{
			try {

				// 인코더 output 경로의 파일은 지운다
				for( EncCompleteItem tempEncCompleteItem : listItem ) {
					String filename = Paths.get(tempEncCompleteItem.strEncOutputFilePath).getFileName().toString();
					Path dest = Paths.get(EmsPropertiesReader.get("[TS]시작.io.dest"));
					dest = dest.resolve(Paths.get(tempEncCompleteItem.strEncOutputFilePath).getFileName().toString());
					String from = "\\\\" + system.ip + tempEncCompleteItem.strEncRealOutputFilePath.substring(4).replace("/", "\\");
					String to = Util.ConverteToNWDriveIP(dest.toString(), mountInfoList);
					EMSProcServiceDeleteFileIo delIo = new EMSProcServiceDeleteFileIo(from, null);			
					delIo.setSrcIp(Util.getCallingIP(to));
					delIo.start(null);						
				}
				
			}catch( Exception ex ) {
				logger.error("",ex);
			}
		}
		
	}
	
}
