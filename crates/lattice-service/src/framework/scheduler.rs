use lattice_ops::scheduler::ServiceScheduler;

pub struct ServiceSchedulerComponent {
    scheduler: ServiceScheduler,
}

impl ServiceSchedulerComponent {
    pub fn new(scheduler: ServiceScheduler) -> Self {
        Self { scheduler }
    }

    pub fn scheduler(&self) -> ServiceScheduler {
        self.scheduler.clone()
    }
}
