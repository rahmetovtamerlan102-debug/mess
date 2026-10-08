package com.aitgram.app

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Color
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.Environment
import android.provider.MediaStore
import android.provider.Settings
import android.view.WindowManager
import android.webkit.ConsoleMessage
import android.webkit.JavascriptInterface
import android.webkit.MimeTypeMap
import android.webkit.ValueCallback
import android.webkit.WebChromeClient
import android.webkit.WebResourceRequest
import android.webkit.WebResourceResponse
import android.webkit.WebSettings
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.FileProvider
import androidx.core.view.ViewCompat
import androidx.core.view.WindowCompat
import androidx.core.view.WindowInsetsCompat
import org.json.JSONArray
import org.json.JSONObject
import java.io.File

class MainActivity : AppCompatActivity() {

    private lateinit var web: WebView
    private var chooserCb: ValueCallback<Array<Uri>>? = null
    private var camCb: ValueCallback<Array<Uri>>? = null
    private var camUri: Uri? = null

    private val chooser = registerForActivityResult(
        ActivityResultContracts.StartActivityForResult()
    ) { r ->
        chooserCb?.onReceiveValue(
            WebChromeClient.FileChooserParams.parseResult(r.resultCode, r.data)
        )
        chooserCb = null
    }

    private val camLauncher = registerForActivityResult(
        ActivityResultContracts.StartActivityForResult()
    ) {
        val u = camUri
        camCb?.onReceiveValue(if (u != null) arrayOf(u) else null)
        camCb = null
        camUri = null
    }

    private val galPermLauncher = registerForActivityResult(
        ActivityResultContracts.RequestPermission()
    ) { /* JS сам вызовет hasFiles() через onResume */ }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        // ─── Edge-to-edge ───
        WindowCompat.setDecorFitsSystemWindows(window, false)
        window.statusBarColor = Color.TRANSPARENT
        window.navigationBarColor = Color.TRANSPARENT
        if (Build.VERSION.SDK_INT >= 29) {
            window.isNavigationBarContrastEnforced = false
        }
        if (Build.VERSION.SDK_INT >= 28) {
            window.attributes = window.attributes.apply {
                layoutInDisplayCutoutMode =
                    WindowManager.LayoutParams.LAYOUT_IN_DISPLAY_CUTOUT_MODE_SHORT_EDGES
            }
        }

        web = WebView(this)
        web.setBackgroundColor(Color.BLACK)
        setContentView(web)

        WindowCompat.getInsetsController(window, web).apply {
            isAppearanceLightStatusBars = false
            isAppearanceLightNavigationBars = false
        }

        // ─── Передача инсетов в страницу через setInsets(top, bottom) ───
        ViewCompat.setOnApplyWindowInsetsListener(web) { v, insets ->
            val bars = insets.getInsets(
                WindowInsetsCompat.Type.systemBars() or
                WindowInsetsCompat.Type.displayCutout()
            )
            val ime = insets.getInsets(WindowInsetsCompat.Type.ime())
            val d = resources.displayMetrics.density
            val top = (bars.top / d).toInt()
            // Пока клавиатура открыта — нижний отступ отдаём IME, а не системе
            val bottom = if (ime.bottom > 0) 0 else (bars.bottom / d).toInt()
            v.setPadding(0, 0, 0, ime.bottom)
            v.evaluateJavascript("setInsets($top, $bottom)", null)
            WindowInsetsCompat.CONSUMED
        }

        // ─── WebView настройки ───
        WebView.setWebContentsDebuggingEnabled(true)

        web.settings.apply {
            javaScriptEnabled = true
            domStorageEnabled = true
            databaseEnabled = true
            allowFileAccess = true
            allowContentAccess = true
            mediaPlaybackRequiresUserGesture = false
            cacheMode = WebSettings.LOAD_DEFAULT
            useWideViewPort = true
            loadWithOverviewMode = true
            mixedContentMode = WebSettings.MIXED_CONTENT_ALWAYS_ALLOW
        }

        web.addJavascriptInterface(Gallery(), "AndroidGallery")

        web.webViewClient = object : WebViewClient() {
            override fun shouldInterceptRequest(
                v: WebView,
                r: WebResourceRequest
            ): WebResourceResponse? = intercept(r.url)

            @Deprecated("old api")
            override fun shouldInterceptRequest(
                v: WebView,
                url: String
            ): WebResourceResponse? = intercept(Uri.parse(url))

            override fun onPageFinished(view: WebView, url: String) {
                // На случай, если страница перезагрузилась — принудительно обновим инсеты
                ViewCompat.requestApplyInsets(web)
            }
        }

        web.webChromeClient = object : WebChromeClient() {
            override fun onConsoleMessage(m: ConsoleMessage): Boolean {
                android.util.Log.d("Aitgram", "${m.message()} @${m.lineNumber()}")
                return true
            }

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
                    camLauncher.launch(
                        Intent(MediaStore.ACTION_IMAGE_CAPTURE).apply {
                            putExtra(MediaStore.EXTRA_OUTPUT, camUri)
                            addFlags(
                                Intent.FLAG_GRANT_WRITE_URI_PERMISSION or
                                Intent.FLAG_GRANT_READ_URI_PERMISSION
                            )
                        }
                    )
                } else {
                    chooserCb = cb
                    chooser.launch(p.createIntent())
                }
                return true
            }
        }

        web.loadUrl("https://твой-домен/")   // ← ЗАМЕНИ на свой адрес
    }

    override fun onResume() {
        super.onResume()
        web.evaluateJavascript("window.onFilesPerm&&onFilesPerm()", null)
        web.evaluateJavascript("window.onGalPerm&&onGalPerm()", null)
        ViewCompat.requestApplyInsets(web)   // обновить инсеты при возврате
    }

    @Deprecated("deprecation")
    override fun onBackPressed() {
        if (web.canGoBack()) web.goBack() else super.onBackPressed()
    }

    private fun intercept(u: Uri): WebResourceResponse? {
        if (u.host != "media.local") return null
        val seg = u.pathSegments

        if (seg.size >= 3 && seg[0] == "f" && seg[1] == "d") {
            val root = Environment.getExternalStorageDirectory().canonicalFile
            val rel = Uri.decode(u.encodedPath!!.removePrefix("/f/d/"))
            val f = File(root, rel).canonicalFile
            if (!f.path.startsWith(root.path) || !f.isFile) return null
            val mime = MimeTypeMap.getSingleton()
                .getMimeTypeFromExtension(f.extension.lowercase())
                ?: "application/octet-stream"
            return WebResourceResponse(
                mime, null, 200, "OK",
                mapOf("Access-Control-Allow-Origin" to "*"),
                f.inputStream()
            )
        }
        return null
    }

    inner class Gallery {

        @JavascriptInterface
        fun hasPermission(): Boolean =
            if (Build.VERSION.SDK_INT >= 33)
                checkSelfPermission(Manifest.permission.READ_MEDIA_IMAGES) ==
                    PackageManager.PERMISSION_GRANTED
            else
                checkSelfPermission(Manifest.permission.READ_EXTERNAL_STORAGE) ==
                    PackageManager.PERMISSION_GRANTED

        @JavascriptInterface
        fun requestPermission() = runOnUiThread {
            if (Build.VERSION.SDK_INT >= 33) {
                galPermLauncher.launch(Manifest.permission.READ_MEDIA_IMAGES)
            } else {
                galPermLauncher.launch(Manifest.permission.READ_EXTERNAL_STORAGE)
            }
        }

        @JavascriptInterface
        fun list(off: Int, lim: Int): String {
            val cols = arrayOf(
                MediaStore.MediaColumns._ID,
                MediaStore.MediaColumns.DISPLAY_NAME,
                MediaStore.MediaColumns.MIME_TYPE
            )
            val uri = MediaStore.Files.getContentUri("external")
            val sel = "${MediaStore.MediaColumns.MEDIA_TYPE}=? OR " +
                      "${MediaStore.MediaColumns.MEDIA_TYPE}=?"
            val args = arrayOf(
                MediaStore.Files.FileColumns.MEDIA_TYPE_IMAGE.toString(),
                MediaStore.Files.FileColumns.MEDIA_TYPE_VIDEO.toString()
            )
            val sort = "${MediaStore.MediaColumns.DATE_ADDED} DESC"
            val arr = JSONArray()
            contentResolver.query(uri, cols, sel, args, "$sort LIMIT $lim OFFSET $off")
                ?.use { c ->
                    val idI = c.getColumnIndexOrThrow(MediaStore.MediaColumns._ID)
                    val nI = c.getColumnIndexOrThrow(MediaStore.MediaColumns.DISPLAY_NAME)
                    val mI = c.getColumnIndexOrThrow(MediaStore.MediaColumns.MIME_TYPE)
                    while (c.moveToNext()) {
                        val mt = c.getString(mI) ?: ""
                        arr.put(
                            JSONObject()
                                .put("id", c.getLong(idI))
                                .put("n", c.getString(nI))
                                .put("v", mt.startsWith("video"))
                        )
                    }
                }
            return arr.toString()
        }

        @JavascriptInterface
        fun hasFiles(): Boolean =
            if (Build.VERSION.SDK_INT >= 30)
                Environment.isExternalStorageManager()
            else
                checkSelfPermission(Manifest.permission.READ_EXTERNAL_STORAGE) ==
                    PackageManager.PERMISSION_GRANTED

        @JavascriptInterface
        fun requestFiles() = runOnUiThread {
            if (Build.VERSION.SDK_INT >= 30) {
                startActivity(
                    Intent(
                        Settings.ACTION_MANAGE_APP_ALL_FILES_ACCESS_PERMISSION,
                        Uri.parse("package:$packageName")
                    )
                )
            } else {
                galPermLauncher.launch(Manifest.permission.READ_EXTERNAL_STORAGE)
            }
        }

        @JavascriptInterface
        fun recentFiles(lim: Int): String {
            val root = Environment.getExternalStorageDirectory()
            val arr = JSONArray()
            listOf("Download", "Documents", "Telegram/Telegram Documents")
                .map { File(root, it) }
                .filter { it.isDirectory }
                .flatMap { d ->
                    d.walkTopDown()
                        .maxDepth(2)
                        .filter { it.isFile && !it.isHidden }
                        .toList()
                }
                .sortedByDescending { it.lastModified() }
                .take(lim)
                .forEach {
                    arr.put(
                        JSONObject()
                            .put("p", it.relativeTo(root).path)
                            .put("n", it.name)
                            .put("s", it.length())
                    )
                }
            return arr.toString()
        }
    }
}
