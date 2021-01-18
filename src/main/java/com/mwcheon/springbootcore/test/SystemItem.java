package com.mwcheon.springbootcore.test;

public class SystemItem {
	public long id;	
	public String type; // 장비타입
	public String name; // 장비 alias
	public String ip;
	public String port;
	public String cpuUsage;
	public String memUsage;
	public String nwUsage;
	public String diskUsage;
	public String originPath; // 작업 input 경로
	public String outputPath;
	public String version;
	public int capMax;
	
	public int status;				//장비상태
	public int auto;				//동작구분
		
	public int trans;
	public int alc;
	public int ifr;
	public int index;
	public int drm;
	public int ftp;	
	public String userid;
	public String userpw;	
	public String time;
	public String worker;
	public String note;
	
	public String startDate;
	public String endDate;
	public String shStatus;				//장비상태
	public String shAuto;				//동작구분
	public String usageType; // TV, Mobile, Clip, ...
	public String vendor;
		
	public String shId;
	public String shTrans;
	public String shAlc;
	public String shIfr;
	public String shIndex;
	public String shDrm;
	public String shFtp;
	
	public int assign; // 작업 할당량
	public int uhd_use;
	
	public String strMspsLocalPathIn;
	public String strMspsLocalPathOut;
	public String strAuthorizeID;
	public String strAuthorizePW;
	public String strPortApi;

	public int encMaxAssign;
	public int nAssign; // 작업 할당량

}
