if __name__ == '__main__':
    # from .match import main as main0
    # main0()
    # from .match_us_patent import main as main1us
    # main1us()
    from .match_cn_patent import main as main1cn
    main1cn()
    print('match_cn_patent done')
    from .match_ep_patent import main as main1ep
    main1ep()
    print('match_ep_patent done')
    # from .matched_texts import main as main2
    # main2()
