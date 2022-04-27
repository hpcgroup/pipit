# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np


class TraceData:
    """General Schema for storing a trace after is read using any one of the readers"""

    def __init__(self, definitions, events):
        self.definitions = definitions  # metadata (None for NSight)
        self.events = events
        self.isNSight = self.definitions is None

    def p2p(self, mirrored=False, commType="bytes"):
        """
        Communication Matrix for Peer-to-Peer (P2P) MPI messages

        Arguments:
        1) mirrored:
        boolean to indicate whether the generated communication
        matrix will be mirrored or not (if mirrored, there will be no distinction
        between the sender and receiver between the two axes)

        2) commType:
        string to choose whether the communication volume should be measured
        by bytes transferred between two processes or the number of messages
        sent (two choices - "bytes" or "counts")

        Returns:
        A 2D Numpy Array that represents the communication matrix for all P2P
        messages  of the given trace and can be used to plot a heatmap
        """

        if not self.isNSight:  # have to look into MPI information for NSight
            ranks = set(
                self.events.loc[self.events["Location Group Type"] == "PROCESS"][
                    "Location Group ID"
                ]
            )
            commMatrix = np.zeros(shape=(len(ranks), len(ranks)))

            # are there any more p2p events (what are MPI_Recv_init and MPI_Sendrecv?)
            # that have to be accounted for?
            sendDF = self.events.loc[
                self.events["Event Type"].isin(["MpiSend", "MpiIsend"]),
                ["Location Group ID", "Attributes"],
            ]
            senderRanks = sendDF["Location Group ID"].to_list()
            receiverRanks = (
                sendDF["Attributes"]
                .apply(lambda attrDict: attrDict["receiver"])
                .to_list()
            )

            if commType == "bytes":
                # bytes sent between processes
                msgVolumes = (
                    sendDF["Attributes"]
                    .apply(lambda attrDict: attrDict["msg_length"])
                    .to_list()
                )
            elif commType == "counts":
                # 1 message between pairs of processes for each DataFrame row
                msgVolumes = np.full(len(sendDF), 1)

            for i in range(len(senderRanks)):
                # if this matrix was plotted as a heatmap,
                # y axis would be senders and x axis would be receivers
                commMatrix[senderRanks[i], receiverRanks[i]] += msgVolumes[
                    i
                ]  # adding up all msg volumes
            if mirrored is True:
                # mirrors the message volumes if the user chooses to do so
                # no distinction between sender and receiver
                for i in range(len(senderRanks)):
                    commMatrix[receiverRanks[i], senderRanks[i]] += msgVolumes[i]

            return commMatrix
