""" Pipeline entrypoint. It executes the staging and
the transformation phases of the pipeline.
"""
from jobs.transformation import transformation
from jobs.staging import staging

def main():
    """Main pipeline script definition.
    :return: None
    """
    staging.main()
    transformation.main()


if __name__ == '__main__':
    main()
