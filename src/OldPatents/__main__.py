if __name__ == '__main__':
    from .refine import main as main0
    main0()
    from .replicas import main as main1
    main1()
    from .merge import main as main2
    main2()
