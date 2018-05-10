# The Erlang-C calculations are described step by step below, with parameters,
# lamb = number of calls per hour,
# ts = average call duration (seconds),
# m = number of agents.
# t = The target answer time (seconds) for service level.


import math


def cal_parameters(calls, ts, t, m):
    lamb = calls/3600

# Calculate traffic intensity (u) -
# The term "traffic intensity" comes from the original application of Erlang-C,
# which was for telephone networks, and the volume of calls was described as the "traffic". We need to calculate the
# traffic intensity as a preliminary step to the rest of the calculations.

    u = lamb * ts

#  Calculate agent occupancy (ro) -
# The agent occupancy, or utilisation, is now calculated by dividing the traffic intensity by the number of agents.
# The agent occupancy will be between 0 and 1. If it is not less than 1 then the agents are overloaded, and the
# Erlang-C calculations are not meaningful, and may give negative waiting times.

    enough_agents = False

    while enough_agents == False:

        ro = u / m
        if ro == 1.0:
            m += 1
            ro = u / m

    # Calculate the Erlang-C formula (eq) - Now we can calculate the main Erlang-C formula. This formula looks complicated,
    # but is straightforward to calculate with a few lines of programming. The value of EC(m,u) is needed to calculate the
    # answers we actually want.

        denom = 0
        for k in range(m):
            x = (u ** k) / math.factorial(k)
            denom += x

        eq = ((u ** m)/ math.factorial(m))/(((u ** m)/ math.factorial(m)) + ((1-ro)*denom))

    # Calculate probability of waiting (pw) - EC(m,u) is the probability that a call is not answered immediately, and has
    # to wait. This is a probability between 0 and 1, and to express it as a percentage of calls we multiply by 100%.

        pw = eq * 100

    # Calculate average speed of answer/ average waiting time - ASA (tw) - Having calculated EC(m,u) it is quite easy to
    # calculate the average waiting time for a call, which is often referred to as the "Average Speed of Answer" or ASA.
    # We have to remember the time units we used for arrival rate and call duration.

        tw = (eq * ts) / (m*(1-ro))

    # Calculate service level (wt) - Frequently we want to calculate the probability that a call will be answered in less
    # than a target waiting time. The formula for this is given here. Remember that, again, the probability will be on the
    # scale 0 to 1 and should be multiplied by 100 to express it as a percentage.

    # wt = Pr(waiting time <= target answer time)
        wt = (1 - eq * math.exp(-(m-u)*(t/ts)))*100

        print "average waiting time: %0.5f s \nPr(call has to wait): %0.5f%%\nPr(waiting time < average answer time):" \
              " %0.5f%%\nNumber of agents : %d \n" % (tw, pw, wt, m)

        if pw > 100:
            enough_agents = False
            print "Queue is unstable. Not enough Agents"
            print "Actual Pr(call has to wait)> 100%"

            m += 1

        else:
            enough_agents = True


    return

# cal_parameters(720.0, 240.0, 15.0, 55)
cal_parameters(52.0, 124, 20, 2)

# lamb = number of calls per hour,
# ts = average call duration (seconds),
# m = number of agents.
# t = The target answer time (seconds) for service level.





