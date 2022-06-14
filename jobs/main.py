from jobs.staging import staging
from jobs.transformation import transformation


def main():
    """Main ETL script definition.
    :return: None
    """
    staging.main()
    transformation.main()


if __name__ == '__main__':
    main()
