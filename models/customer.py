"""
Customer model for ski resort data.
"""
from dataclasses import dataclass
from typing import Optional, Dict, Any
import random


@dataclass
class Customer:
    """Customer information shared between tickets and passes"""
    name: str = ""
    address: Optional[Dict] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    emergency_contact: Optional[Dict] = None

    @classmethod
    def generate(cls, faker):
        """Generate a customer with faker"""
        state = faker.state_abbr()
        name = faker.name()

        # 20% chance of missing some fields for realism
        has_address = random.random() > 0.2
        has_phone = random.random() > 0.2
        has_email = random.random() > 0.2
        has_emergency = random.random() > 0.2

        address = None
        if has_address:
            address = {
                "STREET_ADDRESS": faker.street_address(),
                "CITY": faker.city(),
                "STATE": state,
                "POSTALCODE": faker.postalcode_in_state(state),
            }

        phone = faker.phone_number() if has_phone else None
        email = faker.email() if has_email else None

        emergency_contact = None
        if has_emergency:
            emergency_contact = {
                "NAME": faker.name(),
                "PHONE": faker.phone_number()
            }

        return cls(
            name=name,
            address=address,
            phone=phone,
            email=email,
            emergency_contact=emergency_contact
        )