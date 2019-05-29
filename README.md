# gossip

gossip is a [Go](http://www.golang.org) library that manages cluster
membership and member failure detection using a gossip based protocol.

The use cases for such a library are far-reaching: all distributed systems
require membership, and gossip is a re-usable solution to managing
cluster membership and node failure detection.

gossip is eventually consistent but converges quickly on average.
The speed at which it converges can be heavily tuned via various knobs
on the protocol. Node failures are detected and network partitions are partially
tolerated by attempting to communicate to potentially dead nodes through
multiple routes.

## Building

If you wish to build gossip you'll need Go version 1.11+ installed.

Please check your installation with:

```
go version
```

## Usage

gossip is surprisingly simple to use. An example is shown below:

```go
// Create the initial gossip from a safe configuration.
list, err := gossip.Create(gossip.DefaultLocalConfig())
if err != nil {
	panic("Failed to create gossip: " + err.Error())
}

// Join an existing cluster by specifying at least one known member.
n, err := list.Join([]string{"1.2.3.4"})
if err != nil {
	panic("Failed to join cluster: " + err.Error())
}

// Ask for members of the cluster
for _, member := range list.Members() {
	fmt.Printf("Member: %s %s\n", member.Name, member.Addr)
}

// Continue doing whatever you need, gossip will maintain membership
// information in the background. Delegates can be used for receiving
// events when members join or leave.
```

The most difficult part of gossip is configuring it since it has many
available knobs in order to tune state propagation delay and convergence times.
gossip provides a default configuration that offers a good starting point,
but errs on the side of caution, choosing values that are optimized for
higher convergence at the cost of higher bandwidth usage.

## Protocol

gossip is based on ["SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol"](http://ieeexplore.ieee.org/document/1028914/). However, we extend the protocol in a number of ways:

* Several extensions are made to increase propagation speed and
convergence rate.
* Another set of extensions, that we call Lifeguard, are made to make gossip more robust in the presence of slow message processing (due to factors such as CPU starvation, and network delay or loss).

For details on all of these extensions, please read our paper "[Lifeguard : SWIM-ing with Situational Awareness](https://arxiv.org/abs/1707.00788)", along with the gossip source.  We welcome any questions related
to the protocol on our issue tracker.

## Reference
The Gossip library references the [memberlist](https://github.com/hashicorp/memberlist) from hashicorp. We make some minor adjustments and refined the example.

## License
Gossip is under the Apache 2.0 license. See the [LICENSE](https://github.com/shimingyah/gossip/blob/master/LICENSE) file for details.
