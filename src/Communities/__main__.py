if __name__ == '__main__':
    # from .build import main as main0
    # main0()
    from .assign_paper import main as main1p
    main1p()
    from .assign_us_patent import main as main1us
    main1us()
    from .assign_cn_patent import main as main1cn
    main1cn()
    from .assign_ep_patent import main as main1ep
    main1ep()
    from .pmid2cmnt import main as main2
    main2()
    from .pmid2year import main as main3
    main3()
    from .sort_by_cmnt import main as main4
    main4()
    # from .get_size_dist import main as main5
    # main5()
    # from .get_annual_genes import main as main6
    # main6()
    from .generate_lohl import main as main7
    main7()
