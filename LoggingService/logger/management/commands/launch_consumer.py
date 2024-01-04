from django.core.management.base import BaseCommand
from logger.consumer_edit_created import ConsumerEditCreated


class Command(BaseCommand):
    def handle(self, *args, **options):
        th = ConsumerEditCreated()
        th.start()
        self.stdout.write("Started Consumer Thread")
