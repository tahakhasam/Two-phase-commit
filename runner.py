import asyncio
from coordinator import Coordinator
from failsafe_coordinator import FailSafeCoordinator
from participant import Participant

choice = int(input('''
	1 : Main Coordinator.
	2 : FailSafeCoordinator.
	3 : Participant.
	'''))

try:
	if choice == 1:
		num_of_participants = int(input('Enter number of participants : '))
		failsafe_address = input('Enter failsafe ip address : ')
		main_coordinator = Coordinator(num_of_participants, failsafe_address)
		asyncio.run(main_coordinator.start_connections())

	if choice == 2:
		num_of_participants = int(input('Enter number of participants : '))
		failsafe_coordinator = FailSafeCoordinator(num_of_participants)
		asyncio.run(failsafe_coordinator.start_connections())

	if choice == 3:
		main_address_host = input('Enter server ip address :  ')
		failsafe_address = input('Enter failsafe ip address : ')
		participant = Participant(main_address_host, failsafe_address)
		asyncio.run(participant.perform_actions())

except KeyboardInterrupt:
	if choice == 1:
		main_coordinator.logger.error('Main Coordinator Stopped.')
	if choice == 2:
		failsafe_coordinator.logger.error('Fail Safe Coordinator Stopped')
	if choice == 3:
		participant.logger.error('Participant interrupted.')
