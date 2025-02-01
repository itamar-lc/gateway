# Copyright 2023 Wirepas Ltd licensed under Apache License, Version 2.0
#
# See file LICENSE for full license details.

import os
import sys
from datetime import datetime, timedelta
import argparse
import logging
import base64
from wirepas_gateway.dbus.dbus_client import BusClient
from wirepas_gateway import __pkg_name__


class MessagesConsumerThread(Thread):
    """
    A class that consumes messages from a Queue and saves them to a file, as well as 
    reports basic statistics to an MQTT topic.
    """
    def __init__(
        self,
        settings,
        message_queue,
        file_path,
        file_prefix,
        historical_days,
        max_storage_size,
        max_block_size,
        save_interval,
        mqtt_topic,
    ):
        super(MessagesConsumerThread, self).__init__()
        self.message_queue = message_queue
        self.file_path = file_path
        self.file_prefix = file_prefix
        self.current_file_index = 0
        self.historical_days = historical_days
        self.max_storage_size = max_storage_size
        self.max_block_size = max_block_size
        self.save_interval = save_interval
        self.mqtt_topic = ""
        self.mqtt_wrapper = None
        if mqtt_topic != "":
            self.mqtt_topic = mqtt_topic.format(gw_id=settings.gateway_id)
            self.mqtt_wrapper = MQTTWrapper(
                settings,
                self._on_mqtt_wrapper_termination_cb,
                self._on_connect,
            )
        self.mqtt_wrapper.start()
        self.last_file_name = ""


    def _on_connect(self):
            """
            Callback used to be informed when the MQTT wrapper has connected
            """
            logging.info("MQTT wrapper connected")
    
        def _on_mqtt_wrapper_termination_cb(self):
            """
            Callback used to be informed when the MQTT wrapper has exited
            It is not a normal situation and better to exit the program
            to have a change to restart from a clean session
            """
            logging.error("MQTT wrapper ends. Terminate the program")

    def run(self):
        while True:
            self._save_messages_to_file_and_delete_outdated_messages()
            sleep(self.save_interval)

    def _dump_queue_to_list(self):
        """
        Reads all messages from the queue and returns them as a list.
        """
        output = []
        while not self.message_queue.empty():
            output.append(self.message_queue.get())
        return output
        
    def _get_log_files_info(self):
        """
        Reads the current files in the log directory and returns their metadata.
        """
        pass
    
    def _delete_outdated_day(self, current_files_metadata):
        """
        Deleting the files corresponding to the day that is older than the historical_days.
        It takes as input the current current_files_metadata, and returns the files_metadata
        for the remaining files.
        """
        
        # First for  which date the file must be deleted 
        now = datetime.now()
        logging.info("Check if a file must be deleted")
        file_suffix_to_remove = (
            now - timedelta(days=(self.historical_days + 1))
        ).strftime("_%d_%m_%Y")
        
        remaining_files_metadata = []

        # Iterate over the files and remove the ones that correspond to the date to remove
        for file_info in current_files_metadata:
            if file_info["datetime"].strftime("_%d_%m_%Y") != file_suffix_to_remove:
                remaining_files_metadata.append(file_info)
                
            # Attempt to remove the file
            file_to_remove = os.path.join(self.file_path, file_info["filename"])
            try:
                logging.debug("Trying to remove file %s", file_to_remove)
                os.remove(file_to_remove)
            except OSError as e:
                logging.error("Error removing file %s: %s", file_to_remove, e)
        
        return remaining_files_metadata

    def _delete_old_files(self):
        """
        Deletes old files under the following two conditions:
            - the files are older than the historical_days parameter
            - the total size of the files exceeds the max_storage_size parameter
        """
        # First get the current saved files, and delete the outdated one. 
        saved_files_metadata = self._get_log_files_info()
        saved_files_metadata = self._delete_outdated_day(saved_files_metadata)
        
        # Calculate the current total saved file sizes. If it is over bound, delete
        # as much data as needed to be under the bound, starting with the oldest files
        # first.
        total_sizes = sum([file_info["size"] for file_info in saved_files_metadata])
        
        if total_sizes > self.max_storage_size:
            # Delete the files starting from the oldest one. We sort the files by the
            # timestamp and the index to ensure that the oldest files are deleted first.
            logging.info("Total size of files is %d bytes", total_sizes)
            saved_files_metadata.sort(key=lambda x: x["datetime"].timestamp() + x["index"])
            
            while total_sizes > self.max_storage_size:
                file_info = saved_files_metadata.pop(0)
                
                logging.info("Removing file %s", file_info["filename"])
                file_path = os.path.join(self.file_path, file_info["filename"])
                
                os.remove(file_path)
                total_sizes -= file_info["size"]

            
    def _send_device_number_message(self, messages):
        """
        Sends a message via mqtt containing the number of devices
        """
        logging.info(f"Sending device number message to topic {self.mqtt_topic}")
        
        if self.mqtt_wrapper is None:
            return
            
        number_of_devices = len({src for src, _, _, _, _ in messages})
        
        logging.info(f"Number of devices: {number_of_devices}")
        payload = dumps({"devices": number_of_devices})
        self.mqtt_wrapper.publish(self.mqtt_topic, payload, qos=1, retain=True)

        
    def _make_file_name(self, proposed_file_name):
        """
        Makes file name unique by adding a number to the end of the file name
        """
        if self._is_file_size_reached(
            proposed_file_name + f"_{self.current_file_index}"
        ):
            self.current_file_index += 1
        return proposed_file_name + f"_{self.current_file_index}"


    def _save_messages_to_file_and_delete_outdated_messages(self):
        """
        Save messages to a file and delete outdated messages.
        """
        self._delete_old_files()

        messages = self._dump_queue_to_list()
        self._send_device_number_message(messages)
        
        if not messages:
            return
            
        timestamp = messages[-1][4]

        # Compute the file name
        file_suffix = datetime.fromtimestamp(timestamp // 1000).strftime("_%d_%m_%Y")
        target_file = os.path.join(self.file_path, self.file_prefix + file_suffix)
        
        target_file = self._make_file_name(target_file)

        with open(target_file, "a") as cur_file:
            for src, src_ep, dst_ep, data, timestamp, sink_id, dst, travel_time, qos, hop_count  in messages:
                cur_file.write(
                    "%d;%x;%d;%d;%s\n"
                    % (timestamp, src, src_ep, dst_ep, base64.b64encode(data), sink_id, dst, travel_time, qos, hop_count)
                )

class LocalHistoryService(BusClient):
    """
    A class that listens on the Dbus, checks incoming messages,
    saves the desired ones to a Queue, and starts the consumption
    of messages in a seperate Thread using MessagesConsumerThread.
    """
    def __init__(self, historical_days=5, file_path="", file_prefix="lhs", endpoints=None, max_storage_size=0,max_block_size=0,save_interval=30,max_queue_size=1000,mqtt_topic="") -> None:

        super(LocalHistoryService, self).__init__(
            ignored_ep_filter=None
        )

        if not file_path:
            current_file_path = os.path.abspath(__file__)
            file_path = str(os.path.dirname(current_file_path))
            
        self._verify_parameters(
            historical_days=historical_days,
            max_storage_size=max_storage_size,
            max_block_size=max_block_size,
            save_interval=save_interval,
            file_path=file_path,
            file_prefix=file_prefix,    
            max_queue_size=max_queue_size
        )
        
        self.historical_days = historical_days
        self.file_path = file_path
        self.file_prefix = file_prefix
        self.endpoints = endpoints if endpoints else []
        self.mqtt_topic = mqtt_topic
        self.message_queue = Queue()
        
        logging.info("Local history service started for %d days for EPs: %s", historical_days, endpoints)
        
        parse = ParserHelper(
            description="Wirepas Gateway Local History service arguments",
            version=transport_version,
        )

        parse.add_file_settings()
        parse.add_mqtt()
        parse.add_gateway_config()
        parse.add_filtering_config()
        parse.add_buffering_settings()
        parse.add_debug_settings()
        parse.add_deprecated_args()

        settings = parse.settings()
        
        self.queue_manager = MessagesConsumerThread(
            settings=settings,
            message_queue=self.message_queue,
            file_path=file_path,
            file_prefix=file_prefix,
            historical_days=historical_days,
            max_block_size=max_storage_size,
            max_storage_size=max_block_size,
            save_interval=save_interval,
            mqtt_topic=self.mqtt_topic,
        )
        self.queue_manager.start()
        logging.info(
            "Local history service started for %d days for EPs: %s with max file size %d and file block size %d",
            historical_days,
            endpoints,
            max_storage_size,
            max_block_size,
        )

    def _verify_parameters(self, 
            historical_days=5,
            max_storage_size=0,
            max_block_size=0,
            save_interval=30,
            max_queue_size=1000,
            file_path="/",
            file_prefix="lhs",
        ) -> None:        
            """
            Verify that none of the given parameters is invalid.
            If it is invalid, print an error and exit the program.
            """
            error = False
            if file_path == "":
                logging.error("No file path provided")
                error = True 
                
            if not os.path.exists(file_path):
                logging.error("Provided file path does not exist")
                error = True
            if not os.path.isdir(file_path):
                logging.error("Provided file path is not a directory")
                error = True
            if max_block_size < 0:
                logging.error("Max block size must be non-negative. To disable the parameter, please pass 0")
                error = True
            if max_storage_size < 0:
                logging.error("Max storage size must be non-negative. To disable the parameter, please pass 0")
                error = True
            if save_interval <= 0:
                logging.error("Save interval must be positive")
                error = True
            if historical_days <= 0:
                logging.error("Historical days must be positive")
                error = True
            if max_queue_size <= 0:
                logging.error("Max queue size must be non-negative. To disable the parameter, please pass 0")
                error = True
            
            if error:
                sys.exit(1)
            

    def on_data_received(
        self,
        sink_id,
        timestamp,
        src,
        dst,
        src_ep,
        dst_ep,
        travel_time,
        qos,
        hop_count,
        data,
    ):
        if dst_ep not in self.endpoints:
            logging.debug("Filtered EPs")
            return
            
        self.message_queue.put((src, src_ep, dst_ep, data, timestamp, sink_id, dst, travel_time, qos, hop_count))

def str2none(value):
    """ Ensures string to bool conversion """
    if value == "":
        return None
    return value


def parse_setting_list(list_setting):
    """ This function parse ep list specified from setting file or cmd line

    Input list has following format [1, 5, 10-15] as a string or list of string
    and is expended as a single list [1, 5, 10, 11, 12, 13, 14, 15]

    Args:
        list_setting(str or list): the list from setting file or cmd line.

    Returns: A single list of ep
    """
    if isinstance(list_setting, str):
        # List is a string from cmd line
        list_setting = list_setting.replace("[", "")
        list_setting = list_setting.replace("]", "")
        list_setting = list_setting.split(",")

    single_list = []
    for ep in list_setting:
        # Check if ep is directly an int
        if isinstance(ep, int):
            if ep < 0 or ep > 255:
                raise SyntaxError("EP out of bound")
            single_list.append(ep)
            continue

        # Check if ep is a single ep as string
        try:
            ep = int(ep)
            if ep < 0 or ep > 255:
                raise SyntaxError("EP out of bound")
            single_list.append(ep)
            continue
        except ValueError:
            # Probably a range
            pass

        # Check if ep is a range
        try:
            ep = ep.replace("'", "")
            lower, upper = ep.split("-")
            lower = int(lower)
            upper = int(upper)
            if lower > upper or lower < 0 or upper > 255:
                raise SyntaxError("Wrong EP range value")

            single_list += list(range(lower, upper + 1))
        except (AttributeError, ValueError):
            raise SyntaxError("Wrong EP range format")

    if len(single_list) == 0:
        single_list = None

    return single_list


if __name__ == "__main__":
    debug_level = os.environ.get("WM_DEBUG_LEVEL", "info")
    # Convert it in upper for logging config
    debug_level = "{0}".format(debug_level.upper())

    # enable its logger
    logging.basicConfig(
        format=f'%(asctime)s | [%(levelname)s] {__pkg_name__}@%(filename)s:%(lineno)d:%(message)s',
        level=debug_level,
        stream=sys.stdout
    )

    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')

    parser.add_argument(
        "--historical_days",
        default=os.environ.get("WM_LHS_NUM_DAYS", 5),
        action="store",
        type=int,
        help="Number of historical days to store (1 file per day)",
    )

    parser.add_argument(
        "--historical_file_path",
        default=os.environ.get("WM_LHS_PATH", ""),
        action="store",
        type=str,
        help="Path to store files",
    )

    parser.add_argument(
        "--endpoints_to_save",
        type=str2none,
        default=os.environ.get("WM_LHS_ENDPOINTS", None),
        help=("Destination endpoints list to keep in history (all if not set)"),
    )
    parser.add_argument(
        "--max_storage_size",
        type=int,
        default=os.environ.get("WM_MAX_STORAGE_SPACE", 500),
        help=("Max storage size for historical files [MB]"),
    )

    parser.add_argument(
        "--max_block_size",
        type=int,
        default=os.environ.get("WM_MAX_BLOCK_SIZE", 20),
        help=("Max block size for historical files [MB]"),
    )

    parser.add_argument(
        "--save_file_interval",
        type=int,
        default=os.environ.get("WM_SAVE_INTERVAL", 30),
        help=("Interval to save historical files [seconds]"),
    )
    
    parser.add_argument(
        "--mqtt_topic",
        type=str2none,
        default=os.environ.get("mqtt_topic", "gw-app/status/{gw_id}/local-history-service"),
        help=("Topic to publish number of reporting devices within the network and given save interval."),
    )


    args = parser.parse_args()
    
    LocalHistoryService(
        historical_days=args.historical_days,
        file_path=args.historical_file_path,
        endpoints=parse_setting_list(args.endpoints_to_save),
        max_block_size=args.max_block_size,
        max_storage_size=args.max_storage_size,
        save_interval=args.save_file_interval,
        mqtt_topic=args.mqtt_topic,
    ).run()
