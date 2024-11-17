from aiohttp import ClientSession, ServerDisconnectedError
import xml.etree.ElementTree as ET
import logging

import traceback
import asyncio
from asyncio_telnet import Telnet


SCAN_DELAY = 0.3
LOGGER = logging.getLogger(__name__)

async def async_can_reach_pdeg(host):
    try:
        async with ClientSession() as session:
            async with session.get(host) as resp:
                return resp.ok
    except:
        return False

class PDEG:
    """
    Representation of a Dynalite PDEG (Philips Dynalite Ethernet Gateway).

    This class provides an interface for managing and interacting with a Dynalite lighting control system. 
    It includes methods for connection management, communication via Telnet, and HTTP-based commands.
    
    Attributes:
        None outfacing

    Methods:
        is_reachable() -> bool:
            Checks if the PDEG device is reachable via HTTP.

        discover() -> bool:
            Discovers the configuration of the PDEG by parsing its `Project.xml` file.

        open_connection() -> None:
            Opens a Telnet connection to the PDEG and starts the communication loops.
            Blocks.

        close_connection() -> None:
            Closes the Telnet connection and stops communication loops.

        write(message: str) -> None:
            Sends a message to the PDEG via the Telnet connection.

        async_write_preset(preset, area, fade, join=0xff) -> None:
            Asynchronously sends a command to change the preset of an area.

        async_write_cl(channel, channel_level, area, fade, join=0xff) -> None:
            Asynchronously sends a command to change the brightness of a specific channel.

        async_write_off(area, fade, join=0xff) -> None:
            Asynchronously sends a command to turn off all lights in an area.

        http_async_set(**kwargs) -> None:
            Sends an HTTP command to the PDEG with specified parameters.

        get_areas() -> dict[int, DyNetArea]:
            Returns a dictionary of all configured areas.


    Internal Attributes:
        _host (str): Host address of the PDEG device.
        _fade_time (int): Default fade time for lighting transitions (milliseconds). Default is 500.
        _areas (dict[int, DyNetArea]): Mapping of area codes to `DyNetArea` objects.
        _tn (Telnet): Telnet connection object.
        _port (int): Port number for the Telnet connection. Default is 23.
        _running (bool): Indicates if the PDEG connection is active.
        _write_queue (asyncio.Queue[bytes]): Queue for Telnet write operations.
        _current_fades (list[str]): List of ongoing fade operations.
        _writer_stopped (asyncio.Event): Event to signal the writer coroutine has stopped.
        _reader_stopped (asyncio.Event): Event to signal the reader coroutine has stopped.


    Internal Methods:
        _writer() -> None:
            Internal coroutine to process messages from the write queue and send them via Telnet.
            Blocks.

        _reader() -> None:
            Internal coroutine to continuously read updates from the PDEG and process them.
            Blocks.

        _parse_xml(tree) -> None:
            Parses the XML configuration from the PDEG and sets up areas, channels, and presets.
    """

    def __init__(self, host, fade = 500, port = 23):
        """
        Initializes a PDEG instance.

        Args:
            host (str): The host address of the PDEG device.
            fade (int, optional): Default fade time for lighting transitions in milliseconds. Defaults to 500.
            port (int, optional): Port number for the Telnet connection. Defaults to 23.

        Notes:
            This constructor initializes internal states, including Telnet connection management 
            and configurations for areas and channels. Use `open_connection()` to establish a connection 
            to the PDEG after instantiation.
        """
        self._host = host      
        self._fade_time = fade 
        self._areas: dict[int, DyNetArea] = {} 
        self._tn = Telnet()
        self._port = port
        self._running = False 
        self._write_queue: asyncio.Queue[bytes] = asyncio.Queue(-1)
        self._current_fades: list[str] = [] 
        self._writer_stopped = asyncio.Event()
        self._reader_stopped = asyncio.Event()

    async def is_reachable(self):
        """
        Checks if the PDEG device is reachable via HTTP.

        Returns:
            bool: `True` if the PDEG is reachable, otherwise `False`.
        """
        return await async_can_reach_pdeg("http://" + self._host) 

    async def discover(self) -> bool:
        """
        Discovers the PDEG configuration by retrieving and parsing its `Project.xml` file.

        Returns:
            bool: Always returns `True` upon successful discovery.

        Raises:
            Exception: If there is an issue with retrieving or parsing the XML configuration.
        """
        query = f"http://{self._host}/Project.xml"
        async with ClientSession() as session:
            async with session.get(query) as resp:
                project_xml = await resp.text()
                await self._parse_xml(ET.fromstring(project_xml))
        return True

    async def open_connection(self):
        """
        Opens a Telnet connection to the PDEG and starts communication loops.
        Blocks.

        This method initializes the writer and reader coroutines for managing Telnet communication.
        
        Raises:
            TelnetError: If the Telnet connection cannot be established.
        """
        self._running = True
        await self._tn.open(self._host, port=self._port)
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._writer())
            tg.create_task(self._reader())
            await self.write("Echo 0")
            await self.write("ReplyOK 0")
            await self.write("Brief")

    async def close_connection(self):
        """
        Closes the Telnet connection and stops all communication loops.

        This method waits for the writer and reader coroutines to shut down gracefully.
        """
        LOGGER.info("Closing Connection")
        self._running = False
        await self.write("")
        await self._writer_stopped.wait()
        await self._reader_stopped.wait()
        await self._tn.close()

    async def _writer(self):
        """
        Internal coroutine to process messages from the write queue and send them via Telnet.
        Blocks.

        This method continuously retrieves messages from the queue and writes them to the PDEG.

        Notes:
            This is an internal method and should not be called directly.
        """
        logger = logging.getLogger(__name__ + "/writer")
        logger.info("Starting up...")
        self._writer_stopped.clear()
        while self._running:
            try:
                logger.debug("Waiting for instruction...") 
                ins = await self._write_queue.get() 
                logger.debug(f"Writing {ins}")
                await self._tn.write(ins)
            except:
                logger.error(f"Error while writing, Stack Trace: \n{traceback.format_exc()}")
        logger.info("Shutting down writer...") 
        self._writer_stopped.set()

    async def _reader(self):
        """
        Internal coroutine to read updates from the PDEG and process them.
        Blocks

        This method continuously reads incoming messages from the PDEG, decodes them, 
        and updates the corresponding areas and channels.

        Notes:
            This is an internal method and should not be called directly.
        """
        logger = logging.getLogger(__name__ + "/reader")
        logger.info("Starting up...")
        self._reader_stopped.clear()

        while self._running:
            try:
                logger.debug("Reading Telnet")
                latest_raw = await self._tn.read_until_eof()
                latest = latest_raw.decode(encoding='ascii', errors='ignore').split("\r\n")
                if len(latest) <= 1:
                    await asyncio.sleep(SCAN_DELAY)
                    continue
                logger.debug("Latest update: " + str(latest))

                for line in latest:
                    line = line.strip()
                    if len(line) == 0:
                        continue
                    cmd = line.split(" ")
                    if len(cmd) == 1:
                        args = []
                    else:
                        args = cmd[1].split(",")
                    match cmd[0]:
                        case "P":
                            area_code = int(args[1], 16)         
                            preset = int(args[0])
                            self._areas[area_code].update_preset(preset)
                        case "CL":
                            channel_code = int(args[0])
                            area_code = int(args[2])
                            brightness = int(args[1], 16)
                            if channel_code == 0:
                                self._areas[area_code].update_brightness(brightness)
                            self._areas[area_code]._channels[channel_code].update_brightness(brightness)
            except:
                logger.error(f"Error while updating, Stack Trace: \n{traceback.format_exc()}")
            await asyncio.sleep(SCAN_DELAY)
        logger.info("Shutting down reader...")
        self._reader_stopped.set()

    async def _parse_xml(self, tree):
        """
        Internal coroutine that parses the XML configuration from the PDEG and sets up areas, channels, and presets.

        Args:
            tree (ElementTree): The root of the XML tree structure.

        Notes:
            This is an internal method and should not be called directly. It is invoked 
            during the discovery process.
        """
        for area_elem in tree.findall("./Area"):
            area = DyNetArea(int(area_elem.attrib["id"]), area_elem.attrib["name"], self)
            for channel_elem in area_elem.findall("./Channel"):
                area.add_channel(DyNetChannel(int(channel_elem.attrib["id"]), area, channel_elem.attrib["name"], self))

            for preset_elem in area_elem.findall("./Preset"):
                preset = DyNetPreset(int(preset_elem.attrib["id"]), preset_elem.attrib["name"], area)
                area.add_preset(preset)
            self._areas[area._area_code] = area

    async def write(self, message: str):
        """
        Sends a command string to the PDEG via Telnet.

        Args:
            message (str): The command to send to the PDEG. It will be encoded and added to the write queue.
        """
        to_send = (message + "\r\n").encode('ascii', errors='ignore')
        await self._write_queue.put(to_send)

    async def async_write_preset(self, preset, area, fade, join=0xff):
        """
        Asynchronously sends a command to change the preset of a specific area.

        Args:
            preset (int): The preset number to apply.
            area (int): The area code to apply the preset.
            fade (int): The fade time for the preset change in milliseconds.
            join (int, optional): The join value for the command. Defaults to `0xff`.
        """
        await self.write(f"P {preset} {area} {fade} {join}")

    async def async_write_cl(self, channel, channel_level, area, fade, join=0xff):
        """
        Asynchronously sends a command to adjust the brightness of a specific channel.

        Args:
            channel (int): The channel number to adjust.
            channel_level (int): The brightness level to set (0-100).
            area (int): The area code containing the channel.
            fade (int): The fade time for the adjustment in milliseconds.
            join (int, optional): The join value for the command. Defaults to `0xff`.
        """
        await self.write(f"CL {channel} {channel_level} {area} {fade} {join}")

    async def async_write_off(self, area, fade, join=0xff):
        """
        Asynchronously sends a command to turn off all lights in a specific area.

        Args:
            area (int): The area code to turn off.
            fade (int): The fade time for turning off in milliseconds.
            join (int, optional): The join value for the command. Defaults to `0xff`.
        """
        await self.write(f"O {area} {fade} {join}")

    async def http_async_set(self, **kwargs):
        """
        Sends a HTTP to the PDEG's CGI API with specified parameters.

        Args:
            **kwargs: Key-value pairs representing the command parameters to send.

        Notes:
            This method constructs a query string from the provided arguments and sends 
            an HTTP GET request to the PDEG's SetDyNet.cgi CGI API. 
            Should NOT be used if possible as it is rate-limited.
        """
        query = f"http://{self._host}/SetDyNet.cgi?{'&'.join([f'{key}={arg}' for key, arg in kwargs.items()])}"
        try:
            async with ClientSession() as session:
                (await session.get(query)).close()
        except ServerDisconnectedError:
            await asyncio.sleep(0.1)
        except:
            LOGGER.error("Error while connecting to server, stack trace:\n:" + traceback.format_exc())

    def get_areas(self):
        """
        Retrieves all configured areas.

        Returns:
            dict[int, DyNetArea]: A dictionary mapping area codes to `DyNetArea` objects.
        """
        return self._areas
    

class DyNetArea:
    """
    Representation of an area in a Dynalite lighting control system.

    A `DyNetArea` object models a specific area within the system, including its channels, presets, and properties.
    It provides methods for managing and controlling the area's state, such as activating presets or adjusting brightness.

    Attributes:
        current_preset (int): The currently active preset in the area. Defaults to `-1` (none).
    
    Methods:
        get_pdeg() -> PDEG:
            Returns the associated `PDEG` instance.

        get_area_code() -> int:
            Retrieves the unique code of the area.

        get_name() -> str:
            Retrieves the name of the area.

        get_channels() -> dict[int, DyNetChannel]:
            Returns a dictionary of the area's channels.

        get_presets() -> dict[int, DyNetPreset]:
            Returns a dictionary of the area's presets.

        add_channel(channel) -> None:
            Adds a `DyNetChannel` to the area if it belongs to the area.

        add_preset(preset) -> None:
            Adds a `DyNetPreset` to the area if it belongs to the area.

        async_activate_preset(preset, fade) -> None:
            Activates a specified preset in the area with the given fade time.

        async_set_level(level) -> None:
            Sets the brightness level of the area.

        async_turn_off() -> None:
            Turns off all lights in the area.

        update_preset(preset) -> None:
            Updates the current preset for the area.

        update_brightness(brightness) -> None:
            Updates the current brightness level of the area.

    Internal Attributes:
        _area_code (int): The unique identifier for the area.
        _name (str): The name of the area.
        _channels (dict[int, DyNetChannel]): A dictionary mapping channel codes to `DyNetChannel` objects.
        _presets (dict[int, DyNetPreset]): A dictionary mapping preset codes to `DyNetPreset` objects.
        _pdeg (PDEG): Reference to the associated `PDEG` instance.
        _brightness (int): The current brightness level of the area. Defaults to `-1`.
    """

    def __init__(self, area_code: int, name: str, pdeg: PDEG):
        """
        Initializes a DyNetArea instance.

        Args:
            area_code (int): The unique identifier for the area.
            name (str): The name of the area.
            pdeg (PDEG): The associated PDEG instance.
        """
        self._area_code = area_code
        self._name = name
        self._channels: dict[int, DyNetChannel] = {} 
        self._presets: dict[int, DyNetPreset] = {} 
        self._pdeg = pdeg
        self.current_preset = -1
        self._brightness = -1

    def get_pdeg(self):
        """
        Retrieves the associated PDEG instance.

        Returns:
            PDEG: The associated PDEG instance.
        """
        return self._pdeg

    def get_area_code(self):
        """
        Retrieves the unique code of the area.

        Returns:
            int: The unique area code.
        """
        return self._area_code

    def get_name(self):
        """
        Retrieves the name of the area.

        Returns:
            str: The name of the area.
        """
        return self._name

    def get_channels(self):
        """
        Returns a dictionary of the area's channels.

        Returns:
            dict[int, DyNetChannel]: Mapping of channel codes to `DyNetChannel` objects.
        """
        return self._channels

    def get_presets(self):
        """
        Returns a dictionary of the area's presets.

        Returns:
            dict[int, DyNetPreset]: Mapping of preset codes to `DyNetPreset` objects.
        """
        return self._presets

    def add_channel(self, channel):
        """
        Adds a DyNetChannel to the area if it belongs to the area.

        Args:
            channel (DyNetChannel): The channel to add.

        Raises:
            ValueError: If the channel does not belong to this area.
        """
        if channel.get_area() == self:
            self._channels[channel._channel_code] = channel

    def add_preset(self, preset):
        """
        Adds a DyNetPreset to the area if it belongs to the area.

        Args:
            preset (DyNetPreset): The preset to add.

        Raises:
            ValueError: If the preset does not belong to this area.
        """
        if preset.get_area() == self:
            self._presets[preset.get_preset_code()] = preset

    async def async_activate_preset(self, preset, fade):
        """
        Activates a specified preset in the area with the given fade time.

        Args:
            preset (int): The code of the preset to activate.
            fade (int): The fade time for the preset change in milliseconds.

        Raises:
            Exception: If the preset activation fails.
        """
        try:
            activation = self._presets[preset].async_activate(fade)
            self.current_preset = preset
            await activation
        except:
            LOGGER.error(f"Couln't set preset: {traceback.format_exc()}")

    async def async_set_level(self, level):
        """
        Sets the brightness level of the area.

        Args:
            level (int): The brightness level to set (0-100).
        """
        await self._pdeg.async_write_cl(0, level, self._area_code, self._pdeg._fade_time)

    async def async_turn_off(self):
        """
        Turns off all lights in the area.
        """
        await self._pdeg.async_write_off(self._area_code, self._pdeg._fade_time)

    def update_preset(self, preset):
        """
        Updates the current preset for the area.

        Args:
            preset (int): The code of the preset to update.

        Raises:
            KeyError: If the specified preset does not exist in the area.
        """
        try:
            self._presets[preset]
            self.current_preset = preset
        except:
            LOGGER.error(f"Error while updating preset in area {self._name}, intended preset {preset}, Stack trace: \n{traceback.format_exc()}")

    def update_brightness(self, brightness):
        """
        Updates the current brightness level of the area.

        Args:
            brightness (int): The new brightness level (0-100)

        Raises:
            ValueError: If the brightness level is invalid.
        """
        try:
            self._brightness = brightness
        except:
            LOGGER.error(f"Error while updating brightness in area {self._name}, intended brightness {brightness}, Stack trace: \n{traceback.format_exc()}")


class DyNetChannel():
    """
    Representation of a channel in a Dynalite lighting control system.

    A `DyNetChannel` object models an individual lighting channel, including its brightness level, associated area, and control mechanisms.
    
    Attributes:
        No outfacing attributes.

    Methods:
        get_name() -> str:
            Retrieves the name of the channel.

        get_area() -> DyNetArea:
            Returns the area to which the channel belongs.

        get_brightness() -> int:
            Returns the current brightness level of the channel.

        get_pdeg() -> PDEG:
            Retrieves the associated PDEG instance.

        is_on() -> bool:
            Checks if the channel is currently on (brightness > 0).

        async_set_level(level: int) -> None:
            Sets the brightness level of the channel asynchronously.

        update_brightness(brightness: int) -> None:
            Updates the current brightness level of the channel.

    Internal Attributes:
        _name (str): The name of the channel.
        _channel_code (int): The unique identifier for the channel.
        _area (DyNetArea): The area to which the channel belongs.
        _brightness (int): The current brightness level of the channel. Defaults to `-1`.
        _pdeg (PDEG): Reference to the associated PDEG instance.
    """

    def __init__(self, channel_code: int, area: DyNetArea, name: str, pdeg: PDEG): 
        """
        Initializes a DyNetChannel instance.

        Args:
            channel_code (int): The unique identifier for the channel.
            area (DyNetArea): The area to which the channel belongs.
            name (str): The name of the channel.
            pdeg (PDEG): The associated PDEG instance.
        """
        self._name = name
        self._channel_code = channel_code
        self._area = area
        self._brightness =  -1
        self._pdeg = pdeg

    def get_name(self):
        """
        Retrieves the name of the channel.

        Returns:
            str: The name of the channel.
        """
        return self._name

    def get_area(self):
        """
        Returns the area to which the channel belongs.

        Returns:
            DyNetArea: The area that contains the channel.
        """
        return self._area

    def get_brightness(self):
        """
        Returns the current brightness level of the channel.

        Returns:
            int: The current brightness level. Defaults to `-1` if not set.
        """
        return self._brightness 

    def get_pdeg(self):
        """
        Retrieves the associated PDEG instance.

        Returns:
            PDEG: The associated PDEG instance.
        """
        return self._pdeg
    
    def is_on(self):
        """
        Checks if the channel is currently on (brightness > 0).

        Returns:
            bool: `True` if the channel is on, `False` otherwise.
        """
        return self._brightness > 0 
    
    async def async_set_level(self, level):
        """
        Sets the brightness level of the channel asynchronously.

        Args:
            level (int): The brightness level to set (0-100).
        """
        await self._pdeg.async_write_cl(self._channel_code, level, self._area._area_code, self._pdeg._fade_time)

    def update_brightness(self, brightness):
        """
        Updates the current brightness level of the channel.

        Args:
            brightness (int): The new brightness level. (0 - 100)

        Raises:
            ValueError: If the brightness level is invalid.
        """
        try:
            self._brightness = brightness
        except:
            LOGGER.error(f"Error while updating brightness in channel {self._name}, intended brightness {brightness}, Stack Trace:\n{traceback.format_exc()}")

class DyNetPreset:
    """
    Representation of a preset in a DyNet lighting control system.

    A `DyNetPreset` object models a preset configuration within an area, including its name, preset code, 
    and associated area. It allows for activation of preset settings.

    Attributes:
        No outfacing attributes.

    Methods:
        get_area() -> DyNetArea:
            Returns the area to which the preset belongs.

        get_preset_code() -> int:
            Retrieves the unique preset code.

        get_name() -> str:
            Retrieves the name of the preset.

        async_activate(fade: int) -> None:
            Activates the preset asynchronously with a fade effect.

    Internal Attributes:
        _name (str): The name of the preset.
        _preset_code (int): The unique identifier for the preset.
        _area (DyNetArea): The area to which the preset belongs.
        _pdeg (PDEG): The associated PDEG instance that controls the preset.
    """

    def __init__(self, preset_code: int, name: str, area: DyNetArea):
        """
        Initializes a DyNetPreset instance.

        Args:
            preset_code (int): The unique identifier for the preset.
            name (str): The name of the preset.
            area (DyNetArea): The area to which the preset belongs.
        """

        self._name = name
        self._preset_code = preset_code
        self._area = area
        self._pdeg = self._area._pdeg

    def get_area(self):
        """
        Returns the area to which the preset belongs.

        Returns:
            DyNetArea: The area associated with the preset.
        """
        return self._area
    
    def get_preset_code(self):
        """
        Retrieves the unique preset code.

        Returns:
            int: The unique preset code.
        """
        return self._preset_code

    def get_name(self):
        """
        Retrieves the name of the preset.

        Returns:
            str: The name of the preset.
        """
        return self._name

    async def async_activate(self, fade):
        """
        Activates the preset asynchronously with a fade effect.

        This method triggers the activation of the preset, applying the fade effect as configured in the PDEG instance.
        """
        await self._pdeg.async_write_preset(self._preset_code, self._area._area_code, self._pdeg._fade_time)
