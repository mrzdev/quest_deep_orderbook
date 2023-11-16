from binance.depthcache import FuturesDepthCacheManager

class FDCManager(FuturesDepthCacheManager):
    """Inherit from FuturesDepthCacheManager class and provide a way to specify the depth argument."""
    def _get_socket(self, depth: str = ''):
        """
        Subscribe to a futures depth data stream, specifying the depth argument.
        :param depth: optional Number of depth entries to return, default '' meaning unlimited
        :type depth: str
        """
        sock = self._bm.futures_depth_socket(symbol=self._symbol, depth = depth)
        return sock