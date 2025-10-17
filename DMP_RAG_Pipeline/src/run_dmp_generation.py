from src.pipeline_manager import PipelineManager
from logger.custom_logger import GLOBAL_LOGGER as log

def main():
    try:
        pm = PipelineManager("config/config.yaml")
        pm.run_generation(force_rebuild=False)
        log.info("🎯 NIH DMP Generation Completed Successfully")
    except Exception as e:
        log.error("❌ DMP generation failed", error=str(e))

if __name__ == "__main__":
    main()
