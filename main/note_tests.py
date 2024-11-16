"""
sudo tc qdisc add dev wlp0s20f3 root netem delay 100ms loss 1%
sudo tc -s qdisc

-run python

sudo tc qdisc del dev wlp0s20f3 root

"""