"""
Configuration and monitoring for async tool usage
"""
import os
import time
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class AsyncConfig:
    """Configuration manager for async tool usage"""
    
    def __init__(self):
        self.config = self._load_config()
        self.performance_metrics = {}
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from environment variables"""
        return {
            'use_async_tools': os.getenv('USE_ASYNC_TOOLS', 'false').lower() == 'true',
            'async_timeout': float(os.getenv('ASYNC_TIMEOUT', '30.0')),
            'max_concurrent_operations': int(os.getenv('MAX_CONCURRENT_OPS', '10')),
            'enable_performance_monitoring': os.getenv('ENABLE_PERF_MONITORING', 'true').lower() == 'true',
            'async_fallback_on_error': os.getenv('ASYNC_FALLBACK_ON_ERROR', 'true').lower() == 'true'
        }
    
    def should_use_async(self) -> bool:
        """Determine if async should be used"""
        return self.config['use_async_tools']
    
    def get_timeout(self) -> float:
        """Get async operation timeout"""
        return self.config['async_timeout']
    
    def get_max_concurrent(self) -> int:
        """Get maximum concurrent operations"""
        return self.config['max_concurrent_operations']
    
    def is_performance_monitoring_enabled(self) -> bool:
        """Check if performance monitoring is enabled"""
        return self.config['enable_performance_monitoring']
    
    def should_fallback_on_error(self) -> bool:
        """Check if should fallback to sync on async errors"""
        return self.config['async_fallback_on_error']

class PerformanceMonitor:
    """Monitor performance of sync vs async operations"""
    
    def __init__(self):
        self.metrics = {
            'sync_operations': {'count': 0, 'total_time': 0.0, 'avg_time': 0.0},
            'async_operations': {'count': 0, 'total_time': 0.0, 'avg_time': 0.0},
            'concurrent_operations': {'count': 0, 'total_time': 0.0, 'avg_time': 0.0}
        }
    
    def start_operation(self, operation_type: str) -> float:
        """Start timing an operation"""
        return time.time()
    
    def end_operation(self, operation_type: str, start_time: float):
        """End timing an operation and update metrics"""
        duration = time.time() - start_time
        
        if operation_type in self.metrics:
            self.metrics[operation_type]['count'] += 1
            self.metrics[operation_type]['total_time'] += duration
            self.metrics[operation_type]['avg_time'] = (
                self.metrics[operation_type]['total_time'] / 
                self.metrics[operation_type]['count']
            )
        
        logger.info(f"Operation {operation_type} completed in {duration:.3f}s")
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary"""
        summary = {}
        for op_type, metrics in self.metrics.items():
            summary[op_type] = {
                'total_operations': metrics['count'],
                'total_time': f"{metrics['total_time']:.3f}s",
                'average_time': f"{metrics['avg_time']:.3f}s"
            }
        
        # Calculate improvement metrics
        if (self.metrics['sync_operations']['count'] > 0 and 
            self.metrics['async_operations']['count'] > 0):
            
            sync_avg = self.metrics['sync_operations']['avg_time']
            async_avg = self.metrics['async_operations']['avg_time']
            
            if sync_avg > 0:
                improvement = ((sync_avg - async_avg) / sync_avg) * 100
                summary['performance_improvement'] = f"{improvement:.1f}% faster with async"
        
        return summary
    
    def log_performance_summary(self):
        """Log performance summary"""
        summary = self.get_performance_summary()
        logger.info("=== Performance Summary ===")
        for key, value in summary.items():
            logger.info(f"{key}: {value}")
        logger.info("==========================")

# Global instances
async_config = AsyncConfig()
performance_monitor = PerformanceMonitor()

def get_async_config() -> Dict[str, Any]:
    """Get async configuration"""
    return {
        'use_async': async_config.should_use_async(),
        'async_available': True,  # We know it's available if this module loads
        'preferred_mode': 'async' if async_config.should_use_async() else 'sync',
        'timeout': async_config.get_timeout(),
        'max_concurrent': async_config.get_max_concurrent(),
        'performance_monitoring': async_config.is_performance_monitoring_enabled()
    }

def monitor_operation(operation_type: str):
    """Decorator to monitor operation performance"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            if performance_monitor.is_performance_monitoring_enabled():
                start_time = performance_monitor.start_operation(operation_type)
                try:
                    result = func(*args, **kwargs)
                    performance_monitor.end_operation(operation_type, start_time)
                    return result
                except Exception as e:
                    performance_monitor.end_operation(operation_type, start_time)
                    raise e
            else:
                return func(*args, **kwargs)
        return wrapper
    return decorator

def log_performance_summary():
    """Log current performance summary"""
    if performance_monitor.is_performance_monitoring_enabled():
        performance_monitor.log_performance_summary()
