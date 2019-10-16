from scrapy.downloadermiddlewares.httpproxy import HttpProxyMiddleware


class HttpProxy(HttpProxyMiddleware):

    @staticmethod
    def proxy_shadowsocks():
        proxy = "http://127.0.0.1:1080"
        return proxy
