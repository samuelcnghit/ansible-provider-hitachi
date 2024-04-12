==========================================
HitachiVantara.BlockStorages Release Notes
==========================================

.. contents:: Topics

v1.0.0
=======

Release Summary
===============
Red Hat Ansible Provider for Hitachi Storage version 1.0.0 is the initial release of this adapter.

New features and enhancements
=============================
The following features and enhancements are new to Ansible Provider for Hitachi Storage
1.0.0:

New VSP Ansible modules:
========================
    - hitachi_block_addChap - Adds a CHAP user on an iSCSI port
    - hitachi_block_addHost - Adds the iSCSI name of the host on the initiator side for the iSCSI target of the specified port
    - hitachi_block_addlun - Adds LUNs to an iSCSI target
    - hitachi_block_changeNickName - Changes the nickname of an iSCSI name
    - hitachi_block_createhg - Creates an iSCSI target
    - hitachi_block_createSI - Creates a ShadowImage pair
    - hitachi_block_createTI_with_gen - Creates a Thin Image pair with an autosplit option
    - hitachi_block_createTI - Creates a Thin Image pair
    - hitachi_block_createVol - Creates a volume
    - hitachi_block_deleteHost - Deletes an iSCSI name from an iSCSI target
    - hitachi_block_deleteVol - Deletes a volume
    - hitachi_block_restoreTI - Restores a Thin Image pair
    - hitachi_block_resyncSI - Resyncs a ShadowImage pair
    - hitachi_block_resyncTI_oldest - Resyncs the oldest Thin Image pair
    - hitachi_block_resyncTI - Resyncs a Thin Image pair
    - hitachi_block_splitSI - Splits a ShadowImage pair
    - hitachi_block_splitTI - Splits a Thin Image pair

New VSP One SDS Block Ansible Modules:
======================================
    - add_chapuser_computeport - Adds a CHAP user to a compute port
    - add_computenode - Adds a compute node
    - add_hbas - Adds an iSCSI name to a compute node
    - add_paths - Adds paths to a compute node 
    - attach_volume - Attaches a volume to a compute node
    - create_chapuser - Creates a CHAP user
    - create_volume - Creates a volume
    - delete_computenode - Deletes a compute node
    - delete_tenant - Deletes a compute node and all volumes attached to that compute node
    - delete_volume - Deletes a volume
    - expand_volume - Expands a volume

