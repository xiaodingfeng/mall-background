package cn.lili.modules.file.plugin.impl;

import cn.lili.common.enums.ResultCode;
import cn.lili.common.exception.ServiceException;
import cn.lili.modules.file.entity.enums.OssEnum;
import cn.lili.modules.file.plugin.FilePlugin;
import cn.lili.modules.system.entity.dto.OssSetting;
import com.google.gson.Gson;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.Region;
import com.qiniu.storage.UploadManager;
import com.qiniu.storage.model.BatchStatus;
import com.qiniu.storage.model.DefaultPutRet;
import com.qiniu.util.Auth;

import java.io.InputStream;
import java.util.List;

public class QiNiuFilePlugin implements FilePlugin {

    private OssSetting ossSetting;

    public QiNiuFilePlugin(OssSetting ossSetting) {
        this.ossSetting = ossSetting;
    }

    @Override
    public OssEnum pluginName() {
        return OssEnum.TENCENT_COS;
    }

    /**
     * 获取oss client
     *
     * @return
     */
    private Auth getAuth() {
        return Auth.create(ossSetting.getQiniuKODOAccessKey(), ossSetting.getQiniuKODOSecretKey());
    }


    /**
     * 获取配置前缀
     *
     * @return
     */
    private String getUrlPrefix() {
        return ossSetting.getQiniuUrlPrefix() + "/";
    }

    @Override
    public String pathUpload(String filePath, String key) {
        Configuration cfg = new Configuration(Region.autoRegion());
        cfg.useHttpsDomains = false;
        UploadManager uploadManager = new UploadManager(cfg);
        Auth auth = getAuth();
        String upToken = auth.uploadToken(ossSetting.getQiniuKODOBucket());
        try {
            Response response = uploadManager.put(filePath, key, upToken);
            //解析上传成功的结果
            DefaultPutRet putRet = new Gson().fromJson(response.bodyString(), DefaultPutRet.class);
            System.out.println(putRet.key);
            System.out.println(putRet.hash);
        } catch (QiniuException ex) {
            Response r = ex.response;
            try {
                System.err.println(r.bodyString());
            } catch (QiniuException ex2) {
                //ignore
            }
            throw new ServiceException(ResultCode.OSS_EXCEPTION_ERROR);
        }
        return getUrlPrefix() + key;
    }

    @Override
    public String inputStreamUpload(InputStream inputStream, String key) {
        Configuration cfg = new Configuration(Region.autoRegion());
        cfg.useHttpsDomains = false;
        UploadManager uploadManager = new UploadManager(cfg);
        Auth auth = getAuth();
        String upToken = auth.uploadToken(ossSetting.getQiniuKODOBucket());
        try {
            Response response = uploadManager.put(inputStream, key, upToken, null, null);
            //解析上传成功的结果
            DefaultPutRet putRet = new Gson().fromJson(response.bodyString(), DefaultPutRet.class);
            System.out.println(putRet.key);
            System.out.println(putRet.hash);
        } catch (QiniuException ex) {
            Response r = ex.response;
            try {
                System.err.println(r.bodyString());
            } catch (QiniuException ex2) {
                //ignore
            }
            throw new ServiceException(ResultCode.OSS_EXCEPTION_ERROR);
        }
        return getUrlPrefix() + key;
    }

    @Override
    public void deleteFile(List<String> keys) {
        Configuration cfg = new Configuration(Region.autoRegion());
        cfg.useHttpsDomains = false;
        Auth auth = getAuth();
        BucketManager bucketManager = new BucketManager(auth, cfg);
        try {
            String[] fileList =  keys.toArray(new String[0]);
            BucketManager.BatchOperations batchOperations = new BucketManager.BatchOperations();
            batchOperations.addDeleteOp(ossSetting.getQiniuKODOBucket(), fileList);
            Response response = bucketManager.batch(batchOperations);
            BatchStatus[] batchStatusList = response.jsonToObject(BatchStatus[].class);
            for (int i = 0; i < fileList.length; i++) {
                BatchStatus status = batchStatusList[i];
                String key = fileList[i];
                System.out.print(key + "\t");
                if (status.code == 200) {
                    System.out.println("delete success");
                } else {
                    System.out.println(status.data.error);
                }
            }
        } catch (QiniuException ex) {
            //如果遇到异常，说明删除失败
            System.err.println(ex.code());
            System.err.println(ex.response.toString());
            throw new ServiceException(ResultCode.OSS_DELETE_ERROR);
        }
    }
}
