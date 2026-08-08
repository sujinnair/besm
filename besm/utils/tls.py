import ssl

import httpx


def tls_client(**kwargs) -> httpx.AsyncClient:
    ctx = ssl.create_default_context()
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return httpx.AsyncClient(verify=ctx, **kwargs)
