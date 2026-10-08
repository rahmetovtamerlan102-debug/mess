package com.aitgram.app

import android.Manifest
import android.content.ContentResolver
import android.content.ContentUris
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Bitmap
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.provider.MediaStore
import android.util.Size
import android.webkit.*
import androidx.activity.ComponentActivity
import androidx.activity.result.ActivityResultLauncher
import androidx.activity.result.contract.ActivityResultContracts
import androidx.core.content.ContextCompat
import androidx.core.content.FileProvider
import org.json.JSONArray
import org.json.JSONObject
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File

class MainActivity : ComponentActivity() {

    private lateinit var web: WebView

    // ─── Chooser (обычный выбор файла) ───
    private var chooserCb: ValueCallback<Array<Uri>>? = null
    private val chooser: ActivityResultLauncher<Intent> =
        registerForActivityResult(ActivityResultContracts.StartActivityForResult()) { r ->
            chooserCb?.onReceiveValue(
                WebChromeClient.FileChooserParams.parseResult(r.resultCode, r.data)
            )
            chooserCb = null
        }

    // ─── Камера (capture) ───
    private var camUri: Uri? = null
    private var camCb: ValueCallback<Array<Uri>>? = null
    private val camLauncher: ActivityResultLauncher<Intent> =
        registerForActivityResult(ActivityResultContracts.StartActivityForResult()) { r ->
            if (r.resultCode == RESULT_OK && camUri != null) {
                camCb?.onReceiveValue(arrayOf(camUri!!))
            } else {
                camCb?.onReceiveValue(null)
            }
            camCb = null
            camUri = null
        }

    // ─── Галерея: разрешения ───
    private val galPermLauncher: ActivityResultLauncher<Array<String>> =
        registerForActivityResult(ActivityResultContracts.RequestMultiplePermissions()) {
            web.post {
                web.evaluateJavascript("window.onGalPerm&&window.onGalPerm()", null)
            }
        }

    // ─── Аудио/видео: разрешения ───
    private var pendingPerm: PermissionRequest? = null
    private val mediaPermLauncher: ActivityResultLauncher<Array<String>> =
        registerForActivityResult(ActivityResultContracts.RequestMultiplePermissions()) { res ->
            pendingPerm?.let { r ->
                if (res.values.all { it }) r.grant(r.resources) else r.deny()
            }
            pendingPerm = null
        }

    // ─── Геолокация ───
    private var geoCb: Pair<String, GeolocationPermissions.Callback>? = null
    private val geoLauncher: ActivityResultLauncher<String> =
        registerForActivityResult(ActivityResultContracts.RequestPermission()) { ok ->
            geoCb?.let { (o, cb) -> cb.invoke(o, ok, false) }
            geoCb = null
        }

    private fun granted(p: String): Boolean =
        ContextCompat.checkSelfPermission(this, p) == PackageManager.PERMISSION_GRANTED

    // ═══════════ JS BRIDGE ═══════════
    inner class Gallery {

        @JavascriptInterface
        fun hasPermission(): Boolean = if (Build.VERSION.SDK_INT >= 33) {
            granted(Manifest.permission.READ_MEDIA_IMAGES) ||
            granted(Manifest.permission.READ_MEDIA_VIDEO) ||
            (Build.VERSION.SDK_INT >= 34 &&
             granted("android.permission.READ_MEDIA_VISUAL_USER_SELECTED"))
        } else {
            granted(Manifest.permission.READ_EXTERNAL_STORAGE)
        }

        @JavascriptInterface
        fun requestPermission() {
            val perms = when {
                Build.VERSION.SDK_INT >= 34 -> arrayOf(
                    Manifest.permission.READ_MEDIA_IMAGES,
                    Manifest.permission.READ_MEDIA_VIDEO,
                    "android.permission.READ_MEDIA_VISUAL_USER_SELECTED"
                )
                Build.VERSION.SDK_INT >= 33 -> arrayOf(
                    Manifest.permission.READ_MEDIA_IMAGES,
                    Manifest.permission.READ_MEDIA_VIDEO
                )
                else -> arrayOf(Manifest.permission.READ_EXTERNAL_STORAGE)
            }
            runOnUiThread { galPermLauncher.launch(perms) }
        }

        @JavascriptInterface
        fun list(off: Int, lim: Int): String {
            return try {
                val args = Bundle().apply {
                    putString(ContentResolver.QUERY_ARG_SQL_SELECTION, "media_type IN (1,3)")
                    putString(ContentResolver.QUERY_ARG_SQL_SORT_ORDER, "date_added DESC")
                    putInt(ContentResolver.QUERY_ARG_LIMIT, lim)
                    putInt(ContentResolver.QUERY_ARG_OFFSET, off)
                }
                val arr = JSONArray()
                contentResolver.query(
                    MediaStore.Files.getContentUri("external"),
                    arrayOf("_id", "media_type"), args, null
                )?.use { c ->
                    while (c.moveToNext()) {
                        arr.put(JSONObject()
                            .put("id", c.getLong(0))
                            .put("v", c.getInt(1) == 3))
                    }
                }
                arr.toString()
            } catch (e: Exception) { "[]" }
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        web = WebView(this)
        setContentView(web)

        web.settings.apply {
            javaScriptEnabled = true
            domStorageEnabled = true
            databaseEnabled = true
            mediaPlaybackRequiresUserGesture = false
            cacheMode = WebSettings.LOAD_DEFAULT
            mixedContentMode = WebSettings.MIXED_CONTENT_NEVER_ALLOW
            allowFileAccess = false
            allowContentAccess = false
            setSupportZoom(false)
            builtInZoomControls = false
        }

        web.addJavascriptInterface(Gallery(), "AndroidGallery")

        web.webViewClient = object : WebViewClient() {

            override fun shouldInterceptRequest(
                view: WebView,
                request: WebResourceRequest
            ): WebResourceResponse? {
                val u = request.url
                if (u.host != "media.local") return null
                val seg = u.pathSegments
                if (seg.size < 3) return null
                val kind = seg[0]
                val isVideo = seg[1] == "v"
                val id = seg[2].toLongOrNull() ?: return null

                val base = if (isVideo)
                    MediaStore.Video.Media.EXTERNAL_CONTENT_URI
                else
                    MediaStore.Images.Media.EXTERNAL_CONTENT_URI
                val uri = ContentUris.withAppendedId(base, id)

                val headers = mapOf(
                    "Access-Control-Allow-Origin" to "*",
                    "Cross-Origin-Resource-Policy" to "cross-origin",
                    "Cache-Control" to "public, max-age=86400"
                )

                return try {
                    if (kind == "t") {
                        if (Build.VERSION.SDK_INT < 29) return null
                        val bmp = contentResolver.loadThumbnail(uri, Size(320, 320), null)
                        val out = ByteArrayOutputStream()
                        bmp.compress(Bitmap.CompressFormat.JPEG, 80, out)
                        WebResourceResponse(
                            "image/jpeg", null, 200, "OK", headers,
                            ByteArrayInputStream(out.toByteArray())
                        )
                    } else {
                        WebResourceResponse(
                            contentResolver.getType(uri) ?: "application/octet-stream",
                            null, 200, "OK", headers,
                            contentResolver.openInputStream(uri)
                        )
                    }
                } catch (e: Exception) {
                    WebResourceResponse(
                        "text/plain", "utf-8", 404, "Not Found", headers,
                        ByteArrayInputStream(ByteArray(0))
                    )
                }
            }
        }

        web.webChromeClient = object : WebChromeClient() {

            override fun onShowFileChooser(
                w: WebView,
                cb: ValueCallback<Array<Uri>>,
                p: FileChooserParams
            ): Boolean {
                chooserCb?.onReceiveValue(null); chooserCb = null
                camCb?.onReceiveValue(null); camCb = null

                if (p.isCaptureEnabled) {
                    val dir = File(cacheDir, "camera").apply { mkdirs() }
                    val file = File(dir, "photo_${System.currentTimeMillis()}.jpg")
                    camUri = FileProvider.getUriForFile(
                        this@MainActivity,
                        "com.aitgram.app.fileprovider",
                        file
                    )
                    camCb = cb
                    camLauncher.launch(Intent(MediaStore.ACTION_IMAGE_CAPTURE).apply {
                        putExtra(MediaStore.EXTRA_OUTPUT, camUri)
                        addFlags(
                            Intent.FLAG_GRANT_WRITE_URI_PERMISSION or
                            Intent.FLAG_GRANT_READ_URI_PERMISSION
                        )
                    })
                } else {
                    chooserCb = cb
                    chooser.launch(p.createIntent())
                }
                return true
            }

            override fun onPermissionRequest(r: PermissionRequest) {
                runOnUiThread {
                    val need = r.resources.mapNotNull {
                        when (it) {
                            PermissionRequest.RESOURCE_AUDIO_CAPTURE -> Manifest.permission.RECORD_AUDIO
                            PermissionRequest.RESOURCE_VIDEO_CAPTURE -> Manifest.permission.CAMERA
                            else -> null
                        }
                    }.filter { !granted(it) }

                    if (need.isEmpty()) {
                        r.grant(r.resources)
                    } else {
                        pendingPerm = r
                        mediaPermLauncher.launch(need.toTypedArray())
                    }
                }
            }

            override fun onGeolocationPermissionsShowPrompt(
                o: String,
                cb: GeolocationPermissions.Callback
            ) {
                if (granted(Manifest.permission.ACCESS_FINE_LOCATION)) {
                    cb.invoke(o, true, false)
                } else {
                    geoCb = o to cb
                    geoLauncher.launch(Manifest.permission.ACCESS_FINE_LOCATION)
                }
            }
        }

        web.loadUrl("https://ss-zfvj.onrender.com")
    }

    override fun onBackPressed() {
        if (web.canGoBack()) web.goBack() else super.onBackPressed()
    }
}
